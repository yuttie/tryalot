from abc import ABCMeta, abstractmethod
import contextlib
from dis import Bytecode
import functools
import hashlib
import inspect
import io
import logging
import os
from pathlib import Path
import pickle
from time import sleep
from typing import Any, List, Sequence
import warnings

import zstandard as zstd


__version__ = '0.3.7'


_logger = logging.getLogger(__name__)


@contextlib.contextmanager
def zstd_open_write(path, *args, **kwargs):
    with open(path, 'wb') as f:
        cctx = zstd.ZstdCompressor(*args, **kwargs)
        with cctx.stream_writer(f) as comp:
            yield comp


@contextlib.contextmanager
def zstd_open_read(path, *args, **kwargs):
    with open(path, 'rb') as f:
        dctx = zstd.ZstdDecompressor(*args, **kwargs)
        with dctx.stream_reader(f) as decomp:
            yield io.BufferedReader(decomp)


class HashSink:
    def __init__(self):
        self._h = hashlib.sha1()

    def write(self, b: bytes):
        self._h.update(b)

    def digest(self):
        return self._h.digest()

    def hexdigest(self):
        return self._h.hexdigest()


def _hash(x):
    sink = HashSink()
    pickle.dump(x, sink, protocol=4)
    return sink.digest()


class Module(metaclass=ABCMeta):
    """A class representing a generator and/or consumer in pipelines"""
    def __init__(self, input_names: Sequence[str], output_names: Sequence[str]):
        self._input_names = input_names
        self._output_names = output_names

    @property
    def name(self) -> str:
        """The module's name"""
        return self.__class__.__name__

    @property
    def input_names(self) -> Sequence[str]:
        """Names of data (products) given to the module"""
        return self._input_names

    @property
    def output_names(self) -> Sequence[str]:
        """Names of data (products) generated from the module"""
        return self._output_names

    @property
    def hash(self):
        """A hash object to be used for versioning of the module"""
        return hashlib.sha1(Bytecode(self.execute, first_line=0).dis().encode('utf-8'))

    @abstractmethod
    def execute(self):
        """The primary method which returns products generated from input"""
        pass


def module(input, output):
    """A decorator which turn a function into a module"""
    def decorator(f):
        class Wrapper(Module):
            def __init__(self):
                super().__init__(input, output)
            @property
            def name(self):
                """The module's name"""
                return f.__name__
            @property
            def hash(self):
                """A hash object to be used for versioning of the module"""
                return hashlib.sha1(Bytecode(f, first_line=0).dis().encode('utf-8'))
            def execute(self, *args, **kwargs):
                return f(*args, **kwargs)
        wrapper = Wrapper()
        return functools.wraps(f)(wrapper)
    return decorator


class ProductType:
    @staticmethod
    def file_ext() -> str:
        pass

    @staticmethod
    def match(data: Any) -> bool:
        pass

    @staticmethod
    def load(path: Path) -> Any:
        pass

    @staticmethod
    def dump(data: Any, path: Path):
        pass


class AnyProductType(ProductType):
    @staticmethod
    def file_ext() -> str:
        return '.pickle.zst'

    @staticmethod
    def match(data: Any) -> bool:
        return True

    @staticmethod
    def load(path: Path) -> Any:
        with zstd_open_read(path) as f:
            return pickle.load(f)

    @staticmethod
    def dump(data: Any, path: Path):
        with zstd_open_write(path, level=19, threads=-1) as f:
            pickle.dump(data, f, protocol=4)


class Context:
    product_types: List[ProductType] = [AnyProductType]

    def __init__(self, product_dir='.products'):
        self._product_dir = product_dir
        self._modules = set()
        self._producer = {}
        _logger.info(f'Using "{self._product_dir}" as a product repository')

    def register_modules(self, *modules):
        for mod in modules:
            if not isinstance(mod, Module):
                warnings.warn(f'Module "{mod.name}" is not an instance of Module', RuntimeWarning)
            self._modules.add(mod)
            for name in mod.output_names:
                if name in self._producer:
                    warnings.warn(f'Producer for "{name}" is already registered')
                self._producer[name] = mod
            _logger.info(f'Module "{mod.name}" has been registered')

    def module(self, input, output):
        decorator = module(input, output)
        def deco(f):
            module = decorator(f)
            self.register_modules(module)
            return module
        return deco

    def _get_path(self, name, runhash):
        return Path(
            self._product_dir,
            runhash,
            name)

    def _lock_runhash_dir(self, runhash):
        path = self._get_path('', runhash + '.lock')
        path.parent.mkdir(parents=True, exist_ok=True)
        first_time = True
        while True:
            try:
                lock = open(path, 'x')
            except FileExistsError:
                if first_time:
                    _logger.info(f'Waiting for lock on "{self._get_path("", runhash)}"...')
                    first_time = False
                sleep(0.1)
            else:
                break
        lock.write(str(os.getpid()))
        return lock

    def _unlock_runhash_dir(self, lock):
        lock.close()
        Path(lock.name).unlink()

    def _has(self, name, runhash):
        path = self._get_path(name, runhash)
        for product_type in self.product_types:
            candidate_path = Path(path.parent, path.name + product_type.file_ext())
            if candidate_path.is_file():
                return True
        return False

    def _get(self, name, runhash, default=None):
        path = self._get_path(name, runhash)
        for product_type in self.product_types:
            candidate_path = Path(path.parent, path.name + product_type.file_ext())
            if candidate_path.is_file():
                path = candidate_path
                return product_type.load(path)
        return default

    def _put(self, name, runhash, data):
        path = self._get_path(name, runhash)
        path.parent.mkdir(parents=True, exist_ok=True)
        for product_type in self.product_types:
            if product_type.match(data):
                path = Path(path.parent, path.name + product_type.file_ext())
                try:
                    product_type.dump(data, path)
                except Exception as e:
                    if path.exists():
                        path.unlink()
                    raise e
                break
        _logger.info(f'Product has been saved as "{path}"')

    def get_runhash(self, module, condition=None):
        if condition is None:
            condition = {}
        # Get runhashes from upstream modules
        upstream_hashes = []
        for name in module.input_names:
            producer = self._producer[name]
            upstream_hash = self.get_runhash(producer, condition)
            upstream_hashes.append(upstream_hash)
        # Get keyword arguments from condition for module
        kwargs = condition.get(module.name, {})
        # Compute runhash that identifies this run
        h = hashlib.sha1()
        h.update(hashlib.sha1(module.name.encode('utf-8')).digest())
        h.update(module.hash.digest())
        h.update(hashlib.sha1(''.join(upstream_hashes).encode('utf-8')).digest())
        h.update(_hash(tuple(sorted(kwargs.items()))))
        runhash = h.hexdigest()
        # Return runhash
        return runhash

    def run(self, module, condition=None):
        _logger.info(f'Running module "{module.name}"')
        if condition is None:
            condition = {}
        # Compute runhash for this run
        runhash = self.get_runhash(module, condition)
        # Lock runhash directory
        lock = self._lock_runhash_dir(runhash)
        try:
            # Execute module if needed
            if all(self._has(name, runhash) for name in module.output_names):
                _logger.info(f'Found cached products of module "{module.name}", skipping execution')
                products = tuple(self._get(name, runhash) for name in module.output_names)
            else:
                # Prepare inputs
                args = []
                for name in module.input_names:
                    producer = self._producer[name]
                    i = producer.output_names.index(name)
                    upstream_products = self.run(producer, condition)
                    args.append(upstream_products[i])
                kwargs = condition.get(module.name, {})
                # Execute module
                _logger.info(f'Executing module "{module.name}"')
                products = module.execute(*args, **kwargs)
                if len(module.output_names) == 1:
                    products = (products, )
                # Store the products
                for name, product in zip(module.output_names, products):
                    self._put(name, runhash, product)
        finally:
            # Unlock runhash directory
            self._unlock_runhash_dir(lock)
        # Return products
        return products

    def compute(self, name, condition=None):
        if condition is None:
            condition = {}
        module = self._producer[name]
        i = module.output_names.index(name)
        products = self.run(module, condition)
        return products[i]


# Additional support for library-specific product types
try:
    import numpy as np

    class NumpyNdarrayProductType(ProductType):
        @staticmethod
        def file_ext() -> str:
            return '.npz'

        @staticmethod
        def match(data: Any) -> bool:
            return isinstance(data, np.ndarray)

        @staticmethod
        def load(path: Path) -> Any:
            with np.load(path) as npz:
                if len(npz.files) == 1:
                    return npz[npz.files[0]]
                else:
                    raise RuntimeError(f'Multiple files were found in npz: {path}')

        @staticmethod
        def dump(data: Any, path: Path):
            np.savez_compressed(path, data)

    Context.product_types.insert(-1, NumpyNdarrayProductType)
except ModuleNotFoundError:
    pass


try:
    import holoviews

    class HoloviewsElementProductType(ProductType):
        @staticmethod
        def file_ext() -> str:
            return '.holoviews-pickle.zst'

        @staticmethod
        def match(data: Any) -> bool:
            return isinstance(data, holoviews.core.element.Element)

        @staticmethod
        def load(path: Path) -> Any:
            with zstd_open_read(path) as f:
                return holoviews.Store.load(f)

        @staticmethod
        def dump(data: Any, path: Path):
            with zstd_open_write(path, level=19, threads=-1) as f:
                holoviews.Store.dump(data, f, protocol=4)

    Context.product_types.insert(-1, HoloviewsElementProductType)
except ModuleNotFoundError:
    pass


try:
    import torch

    class TorchTensorProductType(ProductType):
        @staticmethod
        def file_ext() -> str:
            return '.pt.zst'

        @staticmethod
        def match(data: Any) -> bool:
            return isinstance(data, torch.Tensor)

        @staticmethod
        def load(path: Path) -> Any:
            return torch.load(path)

        @staticmethod
        def dump(data: Any, path: Path):
            torch.save(data, path, pickle_protocol=4)

    Context.product_types.insert(-1, TorchTensorProductType)
except ModuleNotFoundError:
    pass

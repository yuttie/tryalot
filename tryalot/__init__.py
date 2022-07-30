from abc import ABCMeta, abstractmethod
import contextlib
import functools
import hashlib
import io
import logging
import os
from os import PathLike
from pathlib import Path
import pickle
from time import sleep
from typing import Any, Callable, Dict, Generator, List, Optional, Sequence, Tuple, Type, Union
import warnings

from graphviz import Digraph
import zstandard as zstd


__version__ = '0.4.0'


_logger = logging.getLogger(__name__)


@contextlib.contextmanager
def zstd_open_write(path: PathLike, *args, **kwargs) -> Generator[Any, None, None]:  # FIXME: ZstdCompressionWriter is actually returned instead of Any but it is not exported from zstandard
    with open(path, 'wb') as f:
        cctx = zstd.ZstdCompressor(*args, **kwargs)
        with cctx.stream_writer(f) as comp:
            yield comp


@contextlib.contextmanager
def zstd_open_read(path: PathLike, *args, **kwargs) -> Generator[io.BufferedReader, None, None]:
    with open(path, 'rb') as f:
        dctx = zstd.ZstdDecompressor(*args, **kwargs)
        with dctx.stream_reader(f) as decomp:
            yield io.BufferedReader(decomp)


class HashSink:
    def __init__(self):
        self._h = hashlib.sha1()

    def write(self, b: bytes):
        self._h.update(b)

    def digest(self) -> bytes:
        return self._h.digest()

    def hexdigest(self) -> str:
        return self._h.hexdigest()


def _hash(x: Any) -> bytes:
    sink = HashSink()
    pickle.dump(x, sink, protocol=4)
    return sink.digest()


class Module(metaclass=ABCMeta):
    """A class representing a generator and/or consumer in pipelines"""
    def __init__(self, input_names: Sequence[str], output_names: Sequence[str], version: Union[int, str]):
        self._input_names = input_names
        self._output_names = output_names
        self._version = str(version)

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
    def version(self) -> str:
        """A string to be used for versioning of the module"""
        return self._version

    @abstractmethod
    def execute(self) -> Tuple[Any]:
        """The primary method which returns products generated from input"""
        pass


def module(input: Sequence[str], output: Sequence[str], version: Any) -> Callable[[Callable[..., Any]], Module]:
    """A decorator which turn a function into a module"""
    def decorator(f):
        class Wrapper(Module):
            def __init__(self):
                super().__init__(input, output, version)
            @property
            def name(self):
                """The module's name"""
                return f.__name__
            def execute(self, *args, **kwargs):
                return f(*args, **kwargs)
        wrapper = Wrapper()
        return functools.wraps(f)(wrapper)
    return decorator


class ProductType(metaclass=ABCMeta):
    @staticmethod
    @abstractmethod
    def file_ext() -> str:
        pass

    @staticmethod
    @abstractmethod
    def match(data: Any) -> bool:
        pass

    @staticmethod
    @abstractmethod
    def load(path: PathLike) -> Any:
        pass

    @staticmethod
    @abstractmethod
    def dump(data: Any, path: PathLike):
        pass


class AnyProductType(ProductType):
    @staticmethod
    def file_ext() -> str:
        return '.pickle.zst'

    @staticmethod
    def match(data: Any) -> bool:
        return True

    @staticmethod
    def load(path: PathLike) -> Any:
        with zstd_open_read(path) as f:
            return pickle.load(f)

    @staticmethod
    def dump(data: Any, path: PathLike):
        with zstd_open_write(path, level=3, threads=-1) as f:
            pickle.dump(data, f, protocol=4)


class Context:
    product_types: List[Type[ProductType]] = [AnyProductType]

    def __init__(self, product_dir='.products'):
        self._product_dir = product_dir
        self._modules = []
        self._producer = {}
        _logger.debug(f'Using "{self._product_dir}" as a product repository')

    def register_modules(self, *modules):
        for mod in modules:
            if not isinstance(mod, Module):
                warnings.warn(f'Module "{mod.name}" is not an instance of Module', RuntimeWarning)
            self._modules.append(mod)
            for name in mod.output_names:
                if name in self._producer:
                    warnings.warn(f'Producer for "{name}" is already registered')
                self._producer[name] = mod
            _logger.debug(f'Module "{mod.name}" has been registered')

    def module(self, input: Sequence[str], output: Sequence[str], version: Any) -> Callable[[Callable[..., Any]], Module]:
        decorator = module(input, output, version)
        def deco(f):
            module = decorator(f)
            self.register_modules(module)
            return module
        return deco

    def _get_path(self, name: str, runhash: Tuple[str, str]) -> Path:
        return Path(
            self._product_dir,
            runhash[0],
            runhash[1],
            name)

    def _lock_runhash_dir(self, runhash: Tuple[str, str]) -> io.TextIOWrapper:
        path = self._get_path('', (runhash[0], runhash[1] + '.lock'))
        path.parent.mkdir(parents=True, exist_ok=True)
        first_time = True
        while True:
            try:
                lock = open(path, 'x')
            except FileExistsError:
                if first_time:
                    _logger.info(f'Waiting for lock on "{path}"...')
                    first_time = False
                sleep(0.1)
            else:
                break
        lock.write(str(os.getpid()))
        return lock

    def _unlock_runhash_dir(self, lock: io.TextIOWrapper):
        lock.close()
        Path(lock.name).unlink()

    def _has(self, name: str, runhash: Tuple[str, str]) -> bool:
        path = self._get_path(name, runhash)
        for product_type in self.product_types:
            candidate_path = Path(path.parent, path.name + product_type.file_ext())
            if candidate_path.is_file():
                return True
        return False

    def _get(self, name: str, runhash: Tuple[str, str], default: Any=None) -> Any:
        path = self._get_path(name, runhash)
        for product_type in self.product_types:
            candidate_path = Path(path.parent, path.name + product_type.file_ext())
            if candidate_path.is_file():
                path = candidate_path
                return product_type.load(path)
        return default

    def _put(self, name: str, runhash: Tuple[str, str], data: Any):
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
        _logger.debug(f'Product has been saved as "{path}"')

    def get_runhash(self, module: Module, condition: Optional[Dict[str, Any]]=None) -> Tuple[str, str]:
        if condition is None:
            condition = {}
        # Get runhashes from upstream modules
        upstream_hashes = []
        for name in module.input_names:
            producer = self._producer[name]
            _, upstream_hash = self.get_runhash(producer, condition)
            upstream_hashes.append(upstream_hash)
        _logger.debug(f'get_runhash@{module.name}: raw upstream hashes: {upstream_hashes}')
        # Get keyword arguments from condition for module
        kwargs = condition.get(module.name, {})
        _logger.debug(f'get_runhash@{module.name}: kwargs: {kwargs}')
        # Compute runhash that identifies this run
        h = hashlib.sha1()
        h.update(hashlib.sha1(module.name.encode('utf-8')).digest())
        h.update(hashlib.sha1(module.version.encode('utf-8')).digest())
        h.update(hashlib.sha1(''.join(upstream_hashes).encode('utf-8')).digest())
        h.update(_hash(tuple(sorted(kwargs.items()))))
        _logger.debug(f"get_runhash@{module.name}: hash(module name) = {hashlib.sha1(module.name.encode('utf-8')).digest()}")
        _logger.debug(f"get_runhash@{module.name}: hash(module hash) = {hashlib.sha1(module.version.encode('utf-8')).digest()}")
        _logger.debug(f"get_runhash@{module.name}: hash(upstream hashes) = {hashlib.sha1(''.join(upstream_hashes).encode('utf-8')).digest()}")
        _logger.debug(f"get_runhash@{module.name}: hash(kwargs) = {_hash(tuple(sorted(kwargs.items())))}")
        # Return runhash
        _logger.debug(f'get_runhash@{module.name}: runhash = {(module.name, h.hexdigest())}')
        return module.name, h.hexdigest()


    def run(self, module: Module, condition: Optional[Dict[str, Any]]=None) -> Tuple[Any]:
        _logger.debug(f'Running module "{module.name}"')
        if condition is None:
            condition = {}
        # Compute runhash for this run
        runhash = self.get_runhash(module, condition)
        # Lock runhash directory
        lock = self._lock_runhash_dir(runhash)
        try:
            # Execute module if needed
            if all(self._has(name, runhash) for name in module.output_names):
                _logger.debug(f'Found cached products of module "{module.name}", skipping execution')
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

    def compute(self, name: str, condition: Optional[Dict[str, Any]]=None) -> Any:
        if condition is None:
            condition = {}
        module = self._producer[name]
        i = module.output_names.index(name)
        products = self.run(module, condition)
        return products[i]

    def depgraph(self) -> Digraph:
        dot = Digraph(node_attr=dict(fontname='monospace', margin='0,0.08'))
        product_ids = {}
        for i, mod in enumerate(self._modules):
            mid = f'M{i}'
            dot.node(mid, mod.name, shape='invhouse', style='filled', fillcolor='gray', fontcolor='black')
            for output_name in mod.output_names:
                if output_name not in product_ids:
                    pid = f'P{len(product_ids)}'
                    product_ids[output_name] = pid
                    dot.node(pid, output_name, shape='ellipse')
                pid = product_ids[output_name]
                dot.edge(mid, pid)
        for i, mod in enumerate(self._modules):
            mid = f'M{i}'
            for input_name in mod.input_names:
                if input_name not in product_ids:
                    pid = f'P{len(product_ids)}'
                    product_ids[input_name] = pid
                    dot.node(pid, input_name, shape='ellipse', color='red')
                pid = product_ids[input_name]
                dot.edge(pid, mid)
        return dot


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
    import torch

    class TorchTensorProductType(ProductType):
        @staticmethod
        def file_ext() -> str:
            return '.pt'

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

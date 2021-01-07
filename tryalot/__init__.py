from abc import ABCMeta, abstractmethod
import contextlib
import functools
import hashlib
import inspect
import io
import logging
import os
import pickle
import warnings

import numpy as np
import zstandard as zstd


__version__ = '0.3.1'


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
    def __init__(self, input_names, output_names):
        self._input_names = input_names
        self._output_names = output_names

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def input_names(self):
        return self._input_names

    @property
    def output_names(self):
        return self._output_names

    @property
    def hash(self):
        return hashlib.sha1(self.execute.__code__.co_code)

    @abstractmethod
    def execute(self):
        pass


def module(input, output):
    def decorator(f):
        class Wrapper(Module):
            def __init__(self):
                super().__init__(input, output)
            @property
            def name(self):
                return f.__name__
            def execute(self, *args, **kwargs):
                return f(*args, **kwargs)
        wrapper = Wrapper()
        return functools.wraps(f)(wrapper)
    return decorator


class Context:
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
        return os.path.join(
            self._product_dir,
            runhash,
            name)

    def _has(self, name, runhash):
        path = self._get_path(name, runhash)
        return os.path.isfile(path + '.pickle.zst') or os.path.isfile(path + '.npz')

    def _get(self, name, runhash, default=None):
        path = self._get_path(name, runhash)
        if os.path.isfile(path + '.pickle.zst'):
            with zstd_open_read(path + '.pickle.zst') as f:
                return pickle.load(f)
        elif os.path.isfile(path + '.npz'):
            with np.load(path + '.npz') as npz:
                if len(npz.files) == 1:
                    return npz[npz.files[0]]
                else:
                    raise RuntimeError(f'Multiple files were found in npz: {path + ".npz"}')
        else:
            return default

    def _put(self, name, runhash, data):
        path = self._get_path(name, runhash)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if type(data) is np.ndarray:
            path = path + '.npz'
            try:
                np.savez_compressed(path, data)
            except Exception as e:
                if os.path.exists(path):
                    os.remove(path)
                raise e
        else:
            path = path + '.pickle.zst'
            try:
                with zstd_open_write(path, level=19, threads=-1) as f:
                    pickle.dump(data, f, protocol=4)
            except Exception as e:
                if os.path.exists(path):
                    os.remove(path)
                raise e
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
        # Execute module if needed
        runhash = self.get_runhash(module, condition)
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
        # Return products
        return products

    def compute(self, name, condition=None):
        if condition is None:
            condition = {}
        module = self._producer[name]
        i = module.output_names.index(name)
        products = self.run(module, condition)
        return products[i]

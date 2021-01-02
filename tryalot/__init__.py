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


__version__ = '0.2.0'


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
        return hashlib.sha1(inspect.getsource(self.execute).encode('utf-8'))

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

    def register_modules(self, *modules):
        for mod in modules:
            if not isinstance(mod, Module):
                warnings.warn(f'Module "{mod.name}" is not an instance of Module', RuntimeWarning)
            self._modules.add(mod)
            for name in mod.output_names:
                if name in self._producer:
                    raise f'Producer for "{name}" is already registered'
                self._producer[name] = mod

    def module(self, input, output):
        decorator = module(input, output)
        def deco(f):
            module = decorator(f)
            self.register_modules(module)
            return module
        return deco

    def _get_path(self, name, run_hash):
        return os.path.join(
            self._product_dir,
            name,
            run_hash)

    def _has(self, name, run_hash):
        path = self._get_path(name, run_hash)
        return os.path.isfile(path + '.pickle.zst') or os.path.isfile(path + '.npz')

    def _get(self, name, run_hash, default=None):
        path = self._get_path(name, run_hash)
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

    def _put(self, name, run_hash, data):
        path = self._get_path(name, run_hash)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if type(data) is np.ndarray:
            np.savez_compressed(path + '.npz', data)
        else:
            with zstd_open_write(path + '.pickle.zst', level=19, threads=-1) as f:
                pickle.dump(data, f, protocol=4)

    def run(self, module, condition=None):
        if condition is None:
            condition = {}
        # Run upstream modules
        args = []
        upstream_hashes = []
        for name in module.input_names:
            producer = self._producer[name]
            i = producer.output_names.index(name)
            upstream_products, upstream_hash = self.run(producer, condition)
            args.append(upstream_products[i])
            upstream_hashes.append(upstream_hash)
        kwargs = condition.get(module.name, {})
        # Compute a hash that identifies this run
        h = hashlib.sha1()
        h.update(hashlib.sha1(module.name.encode('utf-8')).digest())
        h.update(module.hash.digest())
        h.update(hashlib.sha1(''.join(upstream_hashes).encode('utf-8')).digest())
        h.update(_hash(tuple(sorted(kwargs.items()))))
        run_hash = h.hexdigest()
        # Execute the module if needed
        if all(self._has(name, run_hash) for name in module.output_names):
            products = tuple(self._get(name, run_hash) for name in module.output_names)
        else:
            products = module.execute(*args, **kwargs)
            if len(module.output_names) == 1:
                products = (products, )
            # Store the products
            for name, product in zip(module.output_names, products):
                self._put(name, run_hash, product)
        # Return the result with the hash
        return products, run_hash

    def compute(self, name, condition=None):
        if condition is None:
            condition = {}
        module = self._producer[name]
        i = module.output_names.index(name)
        products, _ = self.run(module, condition)
        return products[i]

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


__version__ = '0.1.1'


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
    def __init__(self, h=hashlib.sha1()):
        self._h = h

    def write(self, b: bytes):
        self._h.update(b)

    def digest(self):
        return self._h.digest()

    def hexdigest(self):
        return self._h.hexdigest()


def _hash(x):
    logger = logging.getLogger(f'{__name__}._hash')
    sink = HashSink()
    pickle.dump(x, sink, protocol=4)
    return sink.hexdigest()


def _hash_args(*args, **kwargs):
    logger = logging.getLogger(f'{__name__}._hash_args')
    normalized_args = (args, tuple(sorted(kwargs.items())))
    logger.debug('hash(args) = %s', _hash(normalized_args[0]))
    logger.debug('hash(kwargs) = %s', _hash(normalized_args[1]))
    return _hash(normalized_args)


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
        return hashlib.sha1(inspect.getsource(self.execute).encode('utf-8')).hexdigest()

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

    def _get_path(self, name, condition):
        module = self._producer[name]
        args = condition.get(module.name)
        args_hash = _hash(tuple(sorted(args.items())))
        return os.path.join(
            self._product_dir,
            name,
            module.name,
            module.hash,
            args_hash)

    def has(self, name, condition):
        path = self._get_path(name, condition)
        return os.path.isfile(path + '.pickle.zst') or os.path.isfile(path + '.npz')

    def get(self, name, condition, default=None):
        path = self._get_path(name, condition)
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

    def put(self, name, data, condition):
        path = self._get_path(name, condition)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if type(data) is np.ndarray:
            np.savez_compressed(path + '.npz', data)
        else:
            with zstd_open_write(path + '.pickle.zst', level=19, threads=-1) as f:
                pickle.dump(data, f, protocol=4)

    def run(self, module, condition={}):
        if all(self.has(name, condition) for name in module.output_names):
            pass
        else:
            # Prepare input data for the module
            for name in module.input_names:
                if not self.has(name, condition):
                    # Recursively run the dependencies
                    producer = self._producer[name]
                    self.run(producer, condition)
            # Execute the module
            args = [self.get(name, condition) for name in module.input_names]
            kwargs = condition.get(module.name, {})
            products = module.execute(*args, **kwargs)
            if len(module.output_names) == 1:
                products = (products, )
            # Store the products
            for name, product in zip(module.output_names, products):
                self.put(name, product, condition)

    def compute(self, name, condition={}):
        module = self._producer[name]
        self.run(name, module, condition)
        return self.get(name, condition)


if __name__ == '__main__':
    @module(input=[], output=['p1_output1', 'p1_output2', 'p1_output3'])
    def process1():
        """This is the docstring for process1."""
        print('Executing process1')
        return 'output1', 'output2', 'output3'

    class Process2(Module):
        """This is the docstring for process2."""
        def __init__(self):
            super().__init__(
                ['p1_output1', 'p1_output2', 'p1_output3'],
                ['p2_output'])

        def execute(self, x, y, z):
            print('Executing process2')
            return x + y + z

    process2 = Process2()

    ctx = Context()
    ctx.register_modules(process1, process2)
    ctx.run(process2)

    print(ctx.get('p2_output', process2))

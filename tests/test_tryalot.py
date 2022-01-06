import tryalot


def test_zstd_open_short(tmp_path):
    import os.path
    data = b'data'
    path = os.path.join(tmp_path, 'file.zst')
    with tryalot.zstd_open_write(path) as f:
        f.write(data)
    assert os.path.isfile(path)
    with tryalot.zstd_open_read(path) as f:
        read_data = f.read()
    assert read_data == data


def test_zstd_open_long_single(tmp_path):
    import os
    import os.path
    data = os.urandom(1 * 1024 * 1024)
    path = os.path.join(tmp_path, 'file.zst')
    with tryalot.zstd_open_write(path) as f:
        f.write(data)
    assert os.path.isfile(path)
    with tryalot.zstd_open_read(path) as f:
        read_data = f.read()
    assert read_data == data


def test_zstd_open_long_multi(tmp_path):
    import os
    import os.path
    data = os.urandom(1 * 1024 * 1024)
    path = os.path.join(tmp_path, 'file.zst')
    with tryalot.zstd_open_write(path) as f:
        for i in range(1024):
            start = 1024 * i
            end = 1024 * (i + 1)
            f.write(data[start:end])
    assert os.path.isfile(path)
    with tryalot.zstd_open_read(path) as f:
        read_data = f.read()
    assert read_data == data


def test_hashsink():
    sink1 = tryalot.HashSink()
    sink1.write(b'hello')
    assert sink1.hexdigest() == sink1.hexdigest()
    sink2 = tryalot.HashSink()
    sink2.write(b'hello')
    assert sink1.hexdigest() == sink2.hexdigest()


def test_hash():
    assert tryalot._hash(1) == tryalot._hash(1)
    assert tryalot._hash(1) != tryalot._hash('1')
    assert tryalot._hash(b'hello') == tryalot._hash(b'hello')
    assert tryalot._hash(b'hello') != tryalot._hash(b'hell0')


def test_decorator():
    @tryalot.module(input=[], output=['output1', 'output2', 'output3'], version=1)
    def process():
        """This is the docstring for process."""
        return 'output1', 'output2', 'output3'

    assert process.__name__ == 'process'
    assert process.__doc__ == 'This is the docstring for process.'
    assert process.input_names == []
    assert process.output_names == ['output1', 'output2', 'output3']


def test_module_name_class():
    class ModuleA(tryalot.Module):
        def __init__(self):
            super().__init__([], [], 0)
        def execute(self):
            pass
    assert ModuleA().name == 'ModuleA'


def test_module_name_func():
    @tryalot.module([], [], 0)
    def module_a():
        pass
    assert module_a.name == 'module_a'


def test_module_version_class():
    class ModuleA(tryalot.Module):
        def __init__(self):
            super().__init__([], [], 0)
        def execute(self):
            pass
    assert ModuleA().version == '0'


def test_module_version_func():
    @tryalot.module([], [], 0)
    def module_a():
        pass
    assert module_a.version == '0'


def test_class_module_runhash(tmp_path):
    ctx = tryalot.Context(tmp_path)

    class Process(tryalot.Module):
        def __init__(self):
            super().__init__([], ['output'], 1)
        def execute(self):
            return 1
    ctx.register_modules(Process())

    output1 = ctx.compute('output')
    runhash1 = ctx.get_runhash(Process())

    class Process(tryalot.Module):
        def __init__(self):
            super().__init__([], ['output'], 2)
        def execute(self):
            return 2
    ctx.register_modules(Process())

    output2 = ctx.compute('output')
    runhash2 = ctx.get_runhash(Process())

    class Process(tryalot.Module):
        def __init__(self):
            super().__init__([], ['output'], 1)
        def execute(self):
            return 1
    ctx.register_modules(Process())

    output3 = ctx.compute('output')
    runhash3 = ctx.get_runhash(Process())

    assert output1 != output2
    assert runhash1 != runhash2
    assert output1 == output3
    assert runhash1 == runhash3


def test_func_module_runhash(tmp_path):
    ctx = tryalot.Context(tmp_path)

    @ctx.module(input=[], output=['output'], version=1)
    def process():
        return 1

    output1 = ctx.compute('output')
    runhash1 = ctx.get_runhash(process)

    @ctx.module(input=[], output=['output'], version=2)
    def process():
        return 2

    output2 = ctx.compute('output')
    runhash2 = ctx.get_runhash(process)

    @ctx.module(input=[], output=['output'], version=1)
    def process():
        return 1

    output3 = ctx.compute('output')
    runhash3 = ctx.get_runhash(process)

    assert output1 != output2
    assert runhash1 != runhash2
    assert output1 == output3
    assert runhash1 == runhash3


def test_pipeline(tmp_path):
    @tryalot.module(input=[], output=['p1_output1', 'p1_output2', 'p1_output3'], version=1)
    def process1():
        """This is the docstring for process1."""
        print('Executing process1')
        return 'output1', 'output2', 'output3'

    class Process2(tryalot.Module):
        """This is the docstring for process2."""
        def __init__(self):
            super().__init__(
                ['p1_output1', 'p1_output2', 'p1_output3'],
                ['p2_output'],
                1)

        def execute(self, x, y, z):
            print('Executing process2')
            return '+'.join([x, y, z])

    ctx = tryalot.Context(tmp_path)
    ctx.register_modules(process1, Process2())

    @ctx.module(['p2_output'], ['p3_output'], 1)
    def process3(w):
        return w + '|' + w

    assert ctx.compute('p3_output') == 'output1+output2+output3|output1+output2+output3'


def test_pipeline_with_condition(tmp_path):
    @tryalot.module(input=[], output=['p1_output1', 'p1_output2', 'p1_output3'], version=1)
    def process1(*, upper_case):
        """This is the docstring for process1."""
        print('Executing process1')
        if upper_case:
            return 'OUTPUT1', 'OUTPUT2', 'OUTPUT3'
        else:
            return 'output1', 'output2', 'output3'

    class Process2(tryalot.Module):
        """This is the docstring for process2."""
        def __init__(self):
            super().__init__(
                ['p1_output1', 'p1_output2', 'p1_output3'],
                ['p2_output'],
                1)

        def execute(self, x, y, z, *, glue):
            print('Executing process2')
            return glue.join([x, y, z])

    ctx = tryalot.Context(tmp_path)
    ctx.register_modules(process1, Process2())

    @ctx.module(['p2_output'], ['p3_output'], version=1)
    def process3(w):
        return w + '|' + w

    assert ctx.compute('p3_output', dict(process1=dict(upper_case=True), Process2=dict(glue='--'))) == 'OUTPUT1--OUTPUT2--OUTPUT3|OUTPUT1--OUTPUT2--OUTPUT3'
    assert ctx.compute('p3_output', dict(process1=dict(upper_case=False), Process2=dict(glue='++'))) == 'output1++output2++output3|output1++output2++output3'


def test_cashing(tmp_path):
    import time

    ctx = tryalot.Context(tmp_path)

    @ctx.module(input=[], output=['p1_output'], version=1)
    def process1():
        """This is the docstring for process1."""
        print('Executing process1')
        return time.perf_counter_ns()

    @ctx.module(input=['p1_output'], output=['p2_output'], version=1)
    def process2(x):
        print('Executing process2')
        return time.perf_counter_ns()

    @ctx.module(input=['p2_output'], output=['p3_output'], version=1)
    def process3(x):
        print('Executing process3')
        return time.perf_counter_ns()

    first_p3 = ctx.compute('p3_output')
    second_p3 = ctx.compute('p3_output')

    assert first_p3 == second_p3


def test_cashing_with_condition(tmp_path):
    import time

    ctx = tryalot.Context(tmp_path)

    @ctx.module(input=[], output=['p1_output'], version=1)
    def process1(*, a):
        """This is the docstring for process1."""
        print('Executing process1')
        return time.perf_counter_ns()

    @ctx.module(input=['p1_output'], output=['p2_output'], version=1)
    def process2(x, *, a):
        print('Executing process2')
        return time.perf_counter_ns()

    @ctx.module(input=['p2_output'], output=['p3_output'], version=1)
    def process3(x):
        print('Executing process3')
        return time.perf_counter_ns()

    first_p3 = ctx.compute('p3_output', dict(process1=dict(a=0), process2=dict(a=0)))
    second_p3 = ctx.compute('p3_output', dict(process1=dict(a=1), process2=dict(a=0)))
    third_p3 = ctx.compute('p3_output', dict(process1=dict(a=0), process2=dict(a=1)))
    forth_p3 = ctx.compute('p3_output', dict(process1=dict(a=1), process2=dict(a=1)))
    fifth_p3 = ctx.compute('p3_output', dict(process1=dict(a=0), process2=dict(a=0)))

    assert first_p3 == fifth_p3
    assert first_p3 < second_p3 < third_p3 < forth_p3


def test_any_product_type(tmp_path):
    ctx = tryalot.Context(tmp_path)

    product = [
        0,
        '1',
        b'2',
        [3],
        {4: 'four'},
        (5, 5.1),
    ]

    @ctx.module(input=[], output=['output'], version=1)
    def process():
        return product

    ctx.compute('output')

    from pathlib import Path
    runhash = ctx.get_runhash(process)
    path = ctx._get_path('output', runhash)
    assert Path(path.parent, path.name + '.pickle.zst').is_file()
    loaded_product = ctx._get('output', runhash)
    assert loaded_product == product


def test_numpy_ndarray_product_type(tmp_path):
    import numpy as np

    ctx = tryalot.Context(tmp_path)

    product = np.array(range(12)).reshape((3, 4))

    @ctx.module(input=[], output=['output'], version=1)
    def process():
        return product

    ctx.compute('output')

    from pathlib import Path
    runhash = ctx.get_runhash(process)
    path = ctx._get_path('output', runhash)
    assert Path(path.parent, path.name + '.npz').is_file()
    loaded_product = ctx._get('output', runhash)
    assert np.array_equal(loaded_product, product)


def test_holoviews_element_product_type(tmp_path):
    import holoviews as hv
    import numpy as np
    hv.extension('matplotlib')

    ctx = tryalot.Context(tmp_path)

    np.random.seed(42)
    coords = [(i, np.random.random()) for i in range(20)]
    product = hv.Scatter(coords).opts(title='Title')

    @ctx.module(input=[], output=['output'], version=1)
    def process():
        return product

    ctx.compute('output')

    from pathlib import Path
    runhash = ctx.get_runhash(process)
    path = ctx._get_path('output', runhash)
    assert Path(path.parent, path.name + '.holoviews-pickle.zst').is_file()
    loaded_product = ctx._get('output', runhash)
    # Compare plot images made from product and loaded_product
    hv.save(product, tmp_path.with_name('product.png'))
    hv.save(loaded_product, tmp_path.with_name('loaded_product.png'))
    with open(tmp_path.with_name('product.png'), 'rb') as f:
        image_product = f.read()
    with open(tmp_path.with_name('loaded_product.png'), 'rb') as f:
        image_loaded_product = f.read()
    assert image_loaded_product == image_product


def test_torch_tensor_product_type(tmp_path):
    import torch

    ctx = tryalot.Context(tmp_path)

    product = torch.tensor(range(12)).reshape((3, 4))

    @ctx.module(input=[], output=['output'], version=1)
    def process():
        return product

    ctx.compute('output')

    from pathlib import Path
    runhash = ctx.get_runhash(process)
    path = ctx._get_path('output', runhash)
    assert Path(path.parent, path.name + '.pt.zst').is_file()
    loaded_product = ctx._get('output', runhash)
    assert torch.equal(loaded_product, product)


def test_reuse_same_function_for_different_input_and_output(tmp_path):
    ctx = tryalot.Context(tmp_path)

    @ctx.module([], ['one'], 1)
    def one():
        return 1

    @ctx.module([], ['three'], 1)
    def three():
        return 3

    @ctx.module([], ['five'], 1)
    def five():
        return 5

    def double(x):
        return 2 * x

    ctx.register_modules(
        tryalot.module(['one'  ], ['two'], 1)(double),
        tryalot.module(['three'], ['six'], 1)(double),
        tryalot.module(['five' ], ['ten'], 1)(double),
    )

    assert ctx.compute('two') == 2
    assert ctx.compute('six') == 6
    assert ctx.compute('ten') == 10


def test_depgraph(tmp_path):
    ctx = tryalot.Context(tmp_path)

    @ctx.module(input=[], output=['p1', 'p2', 'p3', 'p4'], version=1)
    def m1():
        pass

    @ctx.module(input=['p1', 'p2'], output=['p5'], version=1)
    def m2(x):
        pass

    @ctx.module(input=['p5', 'p3'], output=['p6'], version=1)
    def m3(x):
        pass

    @ctx.module(input=['p5'], output=['p7'], version=1)
    def m4(x):
        pass

    @ctx.module(input=['p100'], output=['p101'], version=1)
    def m5(x):
        pass

    source = ctx.depgraph().source
    assert source == '''digraph {
	M0 [label=m1 shape=box]
	P0 [label=p1 shape=ellipse]
	M0 -> P0
	P1 [label=p2 shape=ellipse]
	M0 -> P1
	P2 [label=p3 shape=ellipse]
	M0 -> P2
	P3 [label=p4 shape=ellipse]
	M0 -> P3
	M1 [label=m2 shape=box]
	P4 [label=p5 shape=ellipse]
	M1 -> P4
	M2 [label=m3 shape=box]
	P5 [label=p6 shape=ellipse]
	M2 -> P5
	M3 [label=m4 shape=box]
	P6 [label=p7 shape=ellipse]
	M3 -> P6
	M4 [label=m5 shape=box]
	P7 [label=p101 shape=ellipse]
	M4 -> P7
	P0 -> M1
	P1 -> M1
	P4 -> M2
	P2 -> M2
	P4 -> M3
	P8 [label=p100 color=red shape=ellipse]
	P8 -> M4
}
'''

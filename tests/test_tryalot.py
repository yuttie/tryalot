import tryalot


def test_hashsink():
    sink1 = tryalot.HashSink()
    sink1.write(b'hello')
    assert sink1.hexdigest() == sink1.hexdigest()
    sink2 = tryalot.HashSink()
    sink2.write(b'hello')
    assert sink1.hexdigest() == sink2.hexdigest()


def test_hash():
    assert tryalot._hash(b'hello') == tryalot._hash(b'hello')
    assert tryalot._hash(b'hello') != tryalot._hash(b'hell0')


def test_decorator():
    @tryalot.module(input=[], output=['output1', 'output2', 'output3'])
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
            super().__init__([], [])
        def execute(self):
            pass
    assert ModuleA().name == 'ModuleA'


def test_module_name_func():
    @tryalot.module([], [])
    def module_a():
        pass
    assert module_a.name == 'module_a'


def test_pipeline(tmp_path):
    @tryalot.module(input=[], output=['p1_output1', 'p1_output2', 'p1_output3'])
    def process1():
        """This is the docstring for process1."""
        print('Executing process1')
        return 'output1', 'output2', 'output3'

    class Process2(tryalot.Module):
        """This is the docstring for process2."""
        def __init__(self):
            super().__init__(
                ['p1_output1', 'p1_output2', 'p1_output3'],
                ['p2_output'])

        def execute(self, x, y, z):
            print('Executing process2')
            return '+'.join([x, y, z])

    ctx = tryalot.Context(tmp_path)
    ctx.register_modules(process1, Process2())

    @ctx.module(['p2_output'], ['p3_output'])
    def process3(w):
        return w + '|' + w

    assert ctx.compute('p3_output') == 'output1+output2+output3|output1+output2+output3'


def test_pipeline_with_condition(tmp_path):
    @tryalot.module(input=[], output=['p1_output1', 'p1_output2', 'p1_output3'])
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
                ['p2_output'])

        def execute(self, x, y, z, *, glue):
            print('Executing process2')
            return glue.join([x, y, z])

    ctx = tryalot.Context(tmp_path)
    ctx.register_modules(process1, Process2())

    @ctx.module(['p2_output'], ['p3_output'])
    def process3(w):
        return w + '|' + w

    assert ctx.compute('p3_output', dict(process1=dict(upper_case=True), Process2=dict(glue='--'))) == 'OUTPUT1--OUTPUT2--OUTPUT3|OUTPUT1--OUTPUT2--OUTPUT3'
    assert ctx.compute('p3_output', dict(process1=dict(upper_case=False), Process2=dict(glue='++'))) == 'output1++output2++output3|output1++output2++output3'


def test_cashing(tmp_path):
    import time

    ctx = tryalot.Context(tmp_path)

    @ctx.module(input=[], output=['p1_output'])
    def process1():
        """This is the docstring for process1."""
        print('Executing process1')
        return time.monotonic_ns()

    @ctx.module(input=['p1_output'], output=['p2_output'])
    def process2(x):
        print('Executing process2')
        return time.monotonic_ns()

    @ctx.module(input=['p2_output'], output=['p3_output'])
    def process2(x):
        print('Executing process3')
        return time.monotonic_ns()

    first_p3 = ctx.compute('p3_output')
    second_p3 = ctx.compute('p3_output')

    assert first_p3 == second_p3

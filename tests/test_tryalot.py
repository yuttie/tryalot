import tryalot


def test_hashsink():
    sink1 = tryalot.HashSink()
    sink1.write(b'hello')
    assert sink1.hexdigest() == sink1.hexdigest()
    sink2 = tryalot.HashSink()
    sink2.write(b'hello')
    assert sink1.hexdigest() == sink2.hexdigest()


def test_hash():
    assert tryalot._hash(b'hello') == 'b03cce4f1c3106bf2b70609047096329fe36440e'


def test_decorator():
    @tryalot.module(input=[], output=['output1', 'output2', 'output3'])
    def process():
        """This is the docstring for process."""
        return 'output1', 'output2', 'output3'

    assert process.__name__ == 'process'
    assert process.__doc__ == 'This is the docstring for process.'
    assert process.input_names == []
    assert process.output_names == ['output1', 'output2', 'output3']


def test_simple_pipeline():
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

    process2 = Process2()

    ctx = tryalot.Context()
    ctx.register_modules(process1, process2)
    ctx.run(process2)

    assert ctx.get('p2_output', process2) == 'output1+output2+output3'

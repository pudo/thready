import thready

def test_threaded():
    results = set()
    def f(i):
        results.add(i)
    thready.threaded(range(10), f)
    assert results == set(range(10))

def test_error():
    def f(i):
        raise ValueError
    thready.threaded(range(10), f)

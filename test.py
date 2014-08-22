def test():
    var_outer = {1:1}
    def inner():
        var_outer[1] = var_outer[1] + 1
        print var_outer[1]
    inner()

test()

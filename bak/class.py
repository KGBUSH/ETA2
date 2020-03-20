# -*- coding: utf-8 -*-

class ClassDemo:
    def test_demo(self, **kwargs):
        raise NotImplementedError("my test: not implemented!")


class ChildClass(ClassDemo):
    def test_demo(self, a, b):
        self.b = a
        print("OKOKOOK!")


inst = ChildClass()

inst.test_demo(a=2, b=3)

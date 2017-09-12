class Type(object):
    pass


class IntType(Type):
    pass


class StringType(Type):
    pass


class BoolType(Type):
    pass


class Schema(object):
    def __init__(self, fields, mapping):
        for k in mapping:
            assert k in fields, 'unmatched column in schema: "%s"' % k
        for k in fields:
            assert k in mapping, 'untyped column: "%s"' % k

        self.fields = fields
        self.mapping = mapping
        self.types = [mapping[f] for f in fields]

class Expression(object):
    def __init__(self, children):
        self.children = children

    def execute(self, target):
        raise NotImplementedError

    def typecheck(self, schema):
        raise NotImplementedError

    def propagate_schema(self, schema):
        for expr in self.children:
            expr.propagate_schema(schema)
            expr.typecheck(schema)

    def type(self):
        raise NotImplementedError


class Transformation(object):
    def __init__(self):
        pass

    def stream(self):
        raise NotImplementedError

    def schema(self):
        raise NotImplementedError


class Action(object):
    def __init__(self):
        pass


class Column(Expression):
    def __init__(self, name):
        self.name = name
        self.type = None
        super(Column, self).__init__([])

    def execute(self, target):
        return target[self.name]

    def typecheck(self, schema):
        if not self.name in schema.mapping:
            raise RuntimeError('name "%s" is not found in schema' % self.name)
        self.type = schema.mapping[self.name]

    def type(self):
        assert self.type is not None
        return self.type

    def children(self):
        return []


class Length(Expression):
    def __init__(self, base):
        self.base = base
        super(Length, self).__init__([base])

    def execute(self, target):
        target = self.base.execute(target)
        if target is None:
            return None
        else:
            return len(target)

    def typecheck(self, schema):
        if not self.base.type() == StringType:
            raise RuntimeError('Length expected StringType, found "%s"' % self.base.type())


class Equal(Expression):
    def __init__(self, left, right):
        self.left = left
        self.right = right
        super(Equal, self).__init__([left, right])

    def execute(self, target):
        left_target = self.left.execute(target)
        right_target = self.right.execute(target)
        if left_target is None or right_target is None:
            return None
        else:
            return left_target == right_target

    def typecheck(self, schema):
        if not self.left.type() == self.right.type():
            raise RuntimeError('Equal expected same-type args, found "%s" and "%s"' %
                               (self.left.type(), self.right.type()))


class Load(Transformation):
    def __init__(self, path, mapping):
        self.lines = [line.strip() for line in open(path, 'r')]
        self._schema = Schema(self.lines[0].strip().split(), mapping)
        super(Load, self).__init__()

    def stream(self):
        schema = self.schema()
        cols = range(len(schema.fields))
        for i in range(1, len(self.lines)):
            values = self.lines[i].strip().split()
            row = {}
            for j in cols:
                name = schema.fields[j]
                t = schema.types[j]
                value = values[j]

                if value == 'NA':
                    row[name] = None
                else:
                    if t == IntType:
                        row[name] = int(value)
                    elif t == BoolType:
                        row[name] = bool(value)
                    else:
                        assert t == StringType
                        row[name] = value
            yield row

    def schema(self):
        return self._schema


class Filter(Transformation):
    def __init__(self, base, comparison):
        assert isinstance(base, Transformation)
        assert isinstance(comparison, Expression)
        self.base = base
        self.comparison = comparison
        comparison.propagate_schema(base.schema())
        super(Filter, self).__init__()

    def stream(self):
        for row in self.base.stream():
            pred = self.comparison.execute(row)
            # Missing treated as False
            if pred:
                yield row

    def schema(self):
        return self.base.schema()


class Count(Action):
    def __init__(self, base):
        assert isinstance(base, Transformation)
        self.base = base
        super(Count, self).__init__()

    def compute(self):
        elems = 0
        for row in self.base.stream():
            elems += 1
        return elems


class Print(Action):
    def __init__(self, base):
        assert isinstance(base, Transformation)
        self.base = base
        super(Print, self).__init__()

    def compute(self):
        for element in self.base.stream():
            print(element)


class Sum(Action):
    def __init__(self, base, expr):
        assert isinstance(base, Transformation)
        assert isinstance(expr, Expression)
        expr.propagate_schema(base.schema())
        assert isinstance(expr.type(), IntType), 'Sum requires IntType, found "%s"' % expr.type()
        self.base = base
        self.expr = expr
        super(Sum, self).__init__()

    def compute(self):
        x = 0
        for row in self.base.stream():
            element = self.expr.execute(row)
            if element:
                x += element
        return x


class Mean(Action):
    def __init__(self, base, expr):
        assert isinstance(base, Transformation)
        assert isinstance(expr, Expression)
        expr.propagate_schema(base.schema())
        assert isinstance(expr.type(), IntType), 'Mean requires IntType, found "%s"' % expr.type()
        self.base = base
        self.expr = expr
        super(Mean, self).__init__()

    def compute(self):
        x = 0
        n = 0
        for row in self.base.stream():
            element = self.expr.execute(row)
            if element:
                x += element
                n += 1
        if n > 0:
            return float(x) / n
        else:
            return float('nan')


class Counter(Action):
    def __init__(self, base, expr):
        assert isinstance(base, Transformation)
        assert isinstance(expr, Expression)
        expr.propagate_schema(base.schema())
        self.base = base
        self.expr = expr
        super(Counter, self).__init__()

    def compute(self):
        d = {}
        for row in self.base.stream():
            element = self.expr.execute(row)
            if element in d:
                d[element] += 1
            else:
                d[element] = 1
        return d


class Write(Action):
    def __init__(self, base, path):
        assert isinstance(base, Transformation)
        self.base = base
        self.path = path
        super(Write, self).__init__()

    def compute(self):
        with open(self.path, 'w') as out:
            schema = self.base.schema()
            out.write('\t'.join(schema.fields))
            out.write('\n')

            def process(x):
                if x is None:
                    return 'NA'
                else:
                    return str(x)

            for row in self.base.stream():
                out.write('\t'.join([process(row[col]) for col in schema.fields]))
                out.write('\n')

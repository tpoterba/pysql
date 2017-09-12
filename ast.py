class Type(object):
    Int = 'IntType'
    String = 'StringType'
    Bool = 'BoolType'


class Schema(object):
    def __init__(self, fields, mapping):
        for k in mapping:
            assert k in fields, 'unmatched column in schema: "%s"' % k
        for k in fields:
            assert k in mapping, 'untyped column: "%s"' % k
        assert len(fields) == len(mapping), 'length of fields and mapping is unequal, column names must be unique'

        self.fields = fields
        self.mapping = mapping

        self.types = [mapping[f] for f in fields]


class Expression(object):
    def __init__(self, children):
        self.children = children

    def execute(self, row):
        raise NotImplementedError

    def typecheck(self, schema):
        raise NotImplementedError

    def typecheck_all(self, schema):
        for expr in self.children:
            expr.typecheck_all(schema)
            expr.typecheck(schema)
        self.typecheck(schema)

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
        self._type = None
        super(Column, self).__init__([])

    def execute(self, row):
        return row[self.name]

    def typecheck(self, schema):
        if not self.name in schema.mapping:
            raise RuntimeError('name "%s" is not found in schema' % self.name)
        self._type = schema.mapping[self.name]

    def type(self):
        assert self._type is not None
        return self._type

    def children(self):
        return []


class Length(Expression):
    def __init__(self, base):
        self.base = base
        super(Length, self).__init__([base])

    def execute(self, row):
        target = self.base.execute(row)
        if target is None:
            return None
        else:
            return len(target)

    def typecheck(self, schema):
        if not self.base.type() == Type.String:
            raise RuntimeError('Length expected Type.String, found "%s"' % self.base.type())

    def type(self):
        return Type.Int


class Equal(Expression):
    def __init__(self, left, right):
        self.left = left
        self.right = right
        super(Equal, self).__init__([left, right])

    def execute(self, row):
        left_target = self.left.execute(row)
        right_target = self.right.execute(row)
        if left_target is None or right_target is None:
            return None
        else:
            return left_target == right_target

    def typecheck(self, schema):
        if not self.left.type() == self.right.type():
            raise RuntimeError('Equal expected same-type args, found "%s" and "%s"' %
                               (self.left.type(), self.right.type()))

    def type(self):
        return Type.Bool


class NotEqual(Expression):
    def __init__(self, left, right):
        self.left = left
        self.right = right
        super(NotEqual, self).__init__([left, right])

    def execute(self, row):
        left_target = self.left.execute(row)
        right_target = self.right.execute(row)
        if left_target is None or right_target is None:
            return None
        else:
            return left_target != right_target

    def typecheck(self, schema):
        if not self.left.type() == self.right.type():
            raise RuntimeError('NotEqual expected same-type args, found "%s" and "%s"' %
                               (self.left.type(), self.right.type()))

    def type(self):
        return Type.Bool


class LessThan(Expression):
    def __init__(self, left, right):
        self.left = left
        self.right = right
        super(LessThan, self).__init__([left, right])

    def execute(self, row):
        left_target = self.left.execute(row)
        right_target = self.right.execute(row)
        if left_target is None or right_target is None:
            return None
        else:
            return left_target < right_target

    def typecheck(self, schema):
        if not self.left.type() == Type.Int and self.right.type() == Type.Int:
            raise RuntimeError('LessThan expected "Type.Int" and "Type.Int" args, found "%s" and "%s"' %
                               (self.left.type(), self.right.type()))

    def type(self):
        return Type.Bool


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
                    if t == Type.Int:
                        row[name] = int(value)
                    elif t == Type.Bool:
                        row[name] = bool(value)
                    else:
                        assert t == Type.String
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
        comparison.typecheck_all(base.schema())
        super(Filter, self).__init__()

    def stream(self):
        for row in self.base.stream():
            pred = self.comparison.execute(row)
            # Missing treated as False
            if pred:
                yield row

    def schema(self):
        return self.base.schema()


class Select(Transformation):
    def __init__(self, base, selections):
        '''The type of selections is list of (str, Expression)'''
        assert isinstance(base, Transformation)
        new_fields = []
        new_mapping = {}

        for name, expr in selections:
            new_fields.append(name)
            expr.typecheck_all(base.schema())
            new_mapping[name] = expr.type()

        self._schema = Schema(new_fields, new_mapping)
        self.selections = selections
        self.base = base
        super(Select, self).__init__()

    def stream(self):
        for row in self.base.stream():
            new_row = {}
            for name, expr in self.selections:
                new_row[name] = expr.execute(row)

            yield new_row

    def schema(self):
        return self._schema


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


class Sum(Action):
    def __init__(self, base, expr):
        assert isinstance(base, Transformation)
        assert isinstance(expr, Expression)
        expr.typecheck_all(base.schema())
        assert expr.type() == Type.Int, 'Sum requires Type.Int, found "%s"' % expr.type()
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
        expr.typecheck_all(base.schema())
        assert expr.type() == Type.Int, 'Mean requires Type.Int, found "%s"' % expr.type()
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
        expr.typecheck_all(base.schema())
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
        if self.path == 'stdout':
            import sys
            out = sys.stdout
        else:
            out = open(self.path, 'w')
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

        if self.path != 'stdout':
            out.close()


class Collect(Action):
    def __init__(self, base):
        assert isinstance(base, Transformation)
        self.base = base
        super(Collect, self).__init__()

    def compute(self):
        rows = []
        for row in self.base.stream():
            rows.append(row)
        return rows

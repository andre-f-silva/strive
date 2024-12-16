from enum import Enum

import z3


class System:
    def __init__(self, microservices, non_conflict_units=None):
        self.microservices = microservices
        self.non_conflict_units = non_conflict_units

    def __str__(self):
        return f"System with: {self.microservices}"

    def get_bt_of_unit(self, unit):

        for m in self.microservices:
            for bt in m.business_transactions:
                for u in bt.business_transaction_units:
                    if u == unit:
                        return bt

        return None

    def get_bt_of_unit_label(self, unit_label):

        for m in self.microservices:
            for bt in m.business_transactions:
                for u in bt.business_transaction_units:
                    if u.label == unit_label:
                        return bt

        return None

    def get_m_of_bt(self, bt1):

        for m in self.microservices:
            for bt2 in m.business_transactions:
                if bt1 == bt2:
                    return m

        return None


class Microservice:
    def __init__(self, name, business_transactions):
        self.name = name
        self.business_transactions = business_transactions

    def __str__(self):
        return f"Microservice: {self.name}, " \
               f"business_transactions: {self.business_transactions}"

    def add_bt(self, bt):
        self.business_transactions.append(bt)

    def remove_bt(self, bt):
        self.business_transactions.remove(bt)


class Table:
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns

    def __str__(self):
        return f"Table: {self.name}, " \
               f"columns: {self.columns}"

    def column(self, column_name):
        for c in self.columns:
            if c.name == column_name:
                if c.type == str:
                    return z3.String(self.name + "." + c.name)
                elif c.type == int:
                    return z3.Int(self.name + "." + c.name)
                elif c.type == bool:
                    return z3.Bool(self.name + "." + c.name)
        raise ValueError(f"column {column_name} not found")


class Column:
    def __init__(self, name, type):
        self.name = name
        self.type = type

    def __str__(self):
        return f"Column: {self.name}, {self.type}"

    def __repr__(self):
        return self.__str__()


class BusinessTransaction:
    def __init__(self, name, business_transaction_units, params=[]):
        self.name = name
        self.business_transaction_units = business_transaction_units
        self.params = params

        for u in business_transaction_units:
            u.set_bt_owner(self)

        # validate outputs
        for o in self.get_output():
            found_output = False
            for u in self.business_transaction_units:
                if o.name in list(map(lambda out: out.name, u.get_output())):
                    found_output = True
                    break
            if not found_output:
                raise ValueError(f"Output {o.name} not found")

    def replace_remote_call(self, unit_to_replace, new_units):
        for u in new_units:
            u.set_bt_owner(self)
        replacement_index = self.business_transaction_units.index(unit_to_replace)
        self.business_transaction_units[replacement_index:replacement_index + 1] = new_units

    def __str__(self):
        return f"BusinessTransaction: {self.name}, " \
               f"business_transaction_units : {self.business_transaction_units}"

    def get_input(self):
        return {p for p in self.params if isinstance(p, InputParameter)}

    def get_output(self):
        return {p for p in self.params if isinstance(p, OutputParameter)}

    def get_last_lt_outputs(self, lt):
        last_lts = self.get_lts_before_given_lt(lt)
        return flatten(list(map(lambda last_lt: last_lt.get_output(), last_lts)))

    def get_lts_before_given_lt(self, lt):
        last_lts = self.business_transaction_units[:self.business_transaction_units.index(lt)]
        return last_lts


class BusinessTransactionUnit:

    def __init__(self, params=[], cloned=False):
        self.label = None
        self.params = params
        self.cloned = cloned
        self.bt_owner = None

    def set_label(self, label):
        self.label = label

    def set_cloned(self, cloned):
        self.cloned = cloned

    def set_bt_owner(self, bt):
        self.bt_owner = bt


class LocalTransaction(BusinessTransactionUnit):
    def __init__(self, operations, params=[]):
        super().__init__(params)
        self.ctx = dict()
        self.operations = operations
        for o in operations:
            o.set_lt_owner(self)

        # validate outputs
        for o in self.get_output():
            found_output = False
            for u in self.operations:
                if o.name in list(map(lambda out: out.name, u.get_output())):
                    found_output = True
                    break
            if not found_output:
                raise ValueError(f"Output {o.name} not found")

    def __str__(self):
        #return "{" + str(list(map(lambda o: o.name + "; " + self.bt_owner.name, self.operations))) + "}"
        return str(list(map(lambda o: o.name, self.operations))) + self.bt_owner.name + " (" + self.label + ")"

    def __repr__(self):
        return str(self)

    def get_ctx(self):
        if not self.ctx:
            bt_input = self.bt_owner.get_input()
            add_list_to_ctx(self.ctx, bt_input, self.bt_owner.name)

            last_lt_outputs = self.bt_owner.get_last_lt_outputs(self)
            add_list_to_ctx(self.ctx, last_lt_outputs, self.label)

            for p in self.get_input():
                if p.name not in self.ctx:
                    raise ValueError(f"LT input {p.name} not found in context, bt: {self.bt_owner.name}")

                #if p.name not in list(map(lambda i: i.name, bt_input)) and p.name not in list(map(lambda i: i.name, last_lt_outputs)):
                #    raise ValueError(f"LT input {p.name} not found in context, bt: {self.bt_owner.name}")

        return self.ctx

    def get_output(self):
        return {p for p in self.params if isinstance(p, OutputParameter)}

    def get_input(self):
        return {p for p in self.params if isinstance(p, InputParameter)}

    def get_last_op_outputs(self, op):
        last_ops = self.operations[:self.operations.index(op)]
        return flatten(list(map(lambda last_op: last_op.get_output(), last_ops)))


class RemoteBusinessTransaction(BusinessTransactionUnit):
    def __init__(self, business_transaction):
        super().__init__()
        self.business_transaction = business_transaction

    def __str__(self):
        return str(self.label)

    def __repr__(self):
        return str(self)


class InternalBusinessTransaction(BusinessTransaction):
    def __init__(self, name, business_transaction_units, params=[]):
        super().__init__(name, business_transaction_units, params)


class Operation:
    def __init__(self, name, params, type, table, predicates=[], used_columns=[]):
        self.lt_owner = None
        self.name = name
        self.params = params
        self.type = type
        self.table = table
        self.predicates = predicates
        self.ctx = dict()

        if not used_columns:
            self.used_columns = set(map(lambda c: c.name, self.table.columns))
        else:
            self.used_columns = set(used_columns)

    def __str__(self):
        return f"Operation: {self.name}, " \
               f"parameters: {self.params}, " \
               f"type: {self.type}, " \
               f"table: {self.table}"

    def get_output(self):
        return {p for p in self.params if isinstance(p, OutputParameter)}

    def get_input(self):
        return {p for p in self.params if isinstance(p, InputParameter)}

    def set_lt_owner(self, lt):
        self.lt_owner = lt

    def get_operation_ctx(self):

        if not self.ctx:

            lt_ctx = self.lt_owner.get_ctx()
            if lt_ctx:
                self.ctx.update(lt_ctx.copy())

            last_op_outputs = self.lt_owner.get_last_op_outputs(self)
            add_list_to_ctx(self.ctx, last_op_outputs, self.name)

            for p in self.get_input():
                if p.name not in self.ctx:
                    raise ValueError(f"Operation input {p.name} not found in context, {self.name}")

        return self.ctx


class Parameter:
    def __init__(self, name, type):
        self.name = name
        self.type = type


class InputParameter(Parameter):
    pass


class OutputParameter(Parameter):
    pass


class OperationType(Enum):
    READ = "READ"
    WRITE = "WRITE"


def add_list_to_ctx(ctx, list, label):
    for i in list:
        if i.type == int:
            ctx[i.name] = z3.Int(i.name + "_" + label)
        elif i.type == str:
            ctx[i.name] = z3.String(i.name + "_" + label)
        elif i.type == bool:
            ctx[i.name] = z3.Bool(i.name + "_" + label)
        else:
            raise NotImplementedError("")


def flatten(l):
    return [item for sublist in l for item in sublist]
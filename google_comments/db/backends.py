import datetime
import json
import secrets
import sqlite3
from collections import OrderedDict, defaultdict
from functools import cached_property
from hashlib import md5
from sqlite3 import Row

import pytz

from google_comments import PROJECT_PATH

DATABASE = 'scraping'


# class Hash:
#     HASH = 'hash({value})'

#     def __call__(self, text):
#         return md5(text).hexdigest()


class Functions:
    def __init__(self):
        self.backend = None

    def function_sql(self):
        pass


class Lower(Functions):
    def __init__(self, field):
        self.field = field
        super().__init__()

    def __str__(self):
        return f'<{self.__class__.__name__}({self.value})>'

    def function_sql(self):
        sql = self.backend.LOWER.format_map({
            'field': self.field
        })
        return sql


class SQL:
    ALTER_TABLE = 'alter table {table} add column {params}'
    CREATE_TABLE = 'create table if not exists {table} ({fields})'
    DROP_TABLE = 'drop table if exists {table}'
    DELETE = 'delete from {table}'
    INSERT = 'insert into {table} ({fields}) values({values})'
    SELECT = 'select {fields} from {table}'
    UPDATE = 'update {table} set {params}'

    AND = 'and {rhv}'
    OR = 'or {rhv}'

    EQUALITY = '{field}={value}'
    LIKE = '{field} like {conditions}'
    BETWEEN = 'between {lhv} and {rhv}'
    IN = '{field} in ({values})'
    NOT_LIKE = '{field} not like {wildcard}'
    WHERE_CLAUSE = 'where {params}'
    WHERE_NOT = 'where not ({params})'

    WILDCARD_MULTIPLE = '%'
    WILDCARD_SINGLE = '_'

    ASCENDING = '{field} asc'
    DESCENDNIG = '{field} desc'

    ORDER_BY = 'order by {conditions}'
    # UNIQUE_INDEX = 'create unique index {name} ON {table}({fields})'

    LOWER = 'lower({field})'

    @staticmethod
    def quote_value(value):
        if isinstance(value, int):
            return value

        if value.startswith("'"):
            return value
        return f"'{value}'"

    @staticmethod
    def comma_join(values):
        def check_integers(value):
            if isinstance(value, (int, float)):
                return str(value)
            return value
        values = map(check_integers, values)
        return ', '.join(values)

    @staticmethod
    def simple_join(values, space_characters=True):
        def check_integers(value):
            if isinstance(value, (int, float)):
                return str(value)
            return value
        values = map(check_integers, values)

        if space_characters:
            return ' '.join(values)
        return ''.join(values)

    @staticmethod
    def finalize_sql(sql):
        if sql.endswith(';'):
            return sql
        return f'{sql};'

    @staticmethod
    def de_sqlize_statement(sql):
        if sql.endswith(';'):
            return sql.removesuffix(';')
        return sql

    def quote_startswith(self, value):
        """Adds a wildcard to quoted value

        >>> self.quote_startswith(self, 'kendall')
        ... "'kendall%'"
        """
        value = value + '%'
        return self.quote_value(value)

    def quote_endswith(self, value):
        """Adds a wildcard to quoted value

        >>> self.quote_endswith(self, 'kendall')
        ... "'%kendall'"
        """
        value = '%' + value
        return self.quote_value(value)

    def quote_like(self, value):
        """Adds a wildcard to quoted value

        >>> self.quote_like(self, 'kendall')
        ... "'%kendall%'"
        """
        value = f'%{value}%'
        return self.quote_value(value)

    def dict_to_sql(self, data, quote_values=True):
        """Convert a values nested into a dictionnary
        to a sql usable values. The values are quoted
        by default before being returned

        >>> self.dict_to_sql({'name__eq': 'Kendall'})
        ... (['name'], ["'Kendall'"])
        """
        fields = list(data.keys())
        if quote_values:
            quoted_value = list(
                map(lambda x: self.quote_value(x), data.values()))
            return fields, quoted_value
        else:
            return fields, data.values()

    def build_script(self, *sqls):
        return '\n'.join(map(lambda x: self.finalize_sql(x), sqls))

    def decompose_filters(self, **kwargs):
        """Decompose a set of filters to a list of
        key, operator and value list

        >>> self.decompose_filters({'rowid__eq': '1'})
        ... [('rowid', '=', '1')]
        """
        base_filters = {
            'eq': '=',
            'lt': '<',
            'gt': '>',
            'lte': '<=',
            'gte': '>=',
            'contains': 'like',
            'startswith': 'startswith',
            'endswith': 'endswith',
            'range': 'between',
            'ne': '!=',
            'in': 'in',
            'isnull': 'isnull'
        }
        errors = []
        filters_map = []
        for key, value in kwargs.items():
            if '__' not in key:
                key = f'{key}__eq'

            tokens = key.split('__', maxsplit=1)
            if len(tokens) > 2:
                raise ValueError(f'Filter is not valid. Got: {key}')

            lhv, rhv = tokens
            operator = base_filters.get(rhv)
            if operator is None:
                raise ValueError(
                    f'Operator is not recognized. Got: {key}'
                )
            filters_map.append((lhv, operator, value))
        return filters_map

    def build_filters(self, items):
        """Tranform a list of decomposed filters to
        be usable conditions in an sql statement

        >>> self.build_filters([('rowid', '=', '1')])
        ... ["rowid = '1'"]

        >>> self.build_filters([('rowid', 'startswith', '1')])
        ... ["rowid like '1%'"]


        >>> self.build_filters([('url', '=', Lower('url')])
        ... ["lower(url)"]
        """
        built_filters = []
        for item in items:
            field, operator, value = item

            if operator == 'in':
                if not isinstance(value, (tuple, list)):
                    raise ValueError(
                        'The value when using "in" should be a tuple or a list')

                quoted_list_values = (self.quote_value(item) for item in value)
                operator_and_value = self.IN.format_map({
                    'values': self.comma_join(quoted_list_values)
                })
                built_filters.append(operator_and_value)
                continue

            if operator == 'like':
                operator_and_value = self.LIKE.format_map({
                    'field': field,
                    'conditions': self.quote_like(value)
                })
                built_filters.append(operator_and_value)
                continue

            if operator == 'startswith':
                operator_and_value = self.LIKE.format_map({
                    'field': field,
                    'conditions': self.quote_startswith(value)
                })
                built_filters.append(operator_and_value)
                continue

            if operator == 'endswith':
                operator_and_value = self.LIKE.format_map({
                    'field': field,
                    'conditions': self.quote_endswith(value)
                })
                built_filters.append(operator_and_value)
                continue

            if operator == 'range':
                lhv, rhv = value
                operator_and_value = self.BETWEEN.format_map({
                    'lhv': lhv,
                    'rhv': rhv
                })
                built_filters.append(operator_and_value)
                continue

            value = self.quote_value(value)
            built_filters.append(
                self.simple_join((field, operator, value))
            )
        return built_filters

    def build_annotation(self, **conditions):
        function_filters = {}
        for key, function in conditions.items():
            if isinstance(function, Functions):
                function.backend = self
                # Use key as alias instead of column named
                # "lower(value)" in the returned results
                function_filters[key] = self.simple_join(
                    [function.function_sql(), f'as {key}']
                )

        joined_fields = self.comma_join(function_filters.values())
        return [joined_fields]


class SQLiteBackend(SQL):
    """Class that initiates and encapsulates a
    new connection to the database"""

    def __init__(self, database=None):
        if database is None:
            database = ':memory:'
        else:
            database = f'{database}.sqlite'
        self.database = database

        connection = sqlite3.connect(database)
        # connection.create_function('hash', 1, Hash())
        connection.row_factory = BaseRow
        self.connection = connection

    def list_table_columns_sql(self, table):
        sql = f'pragma table_info({table.name})'
        query = Query(self, [sql], table=table)
        query.run()
        return query.result_cache

    def create_table_fields(self, table, columns_to_create):
        field_params = []
        while columns_to_create:
            column_to_create = columns_to_create.pop()
            field = table.fields_map[column_to_create]
            field_params.append(field.field_parameters())

        statements = [self.simple_join(param) for param in field_params]
        for i, statement in enumerate(statements):
            if i > 1:
                statement = f'add table {statement}'
            statements[i] = statement

        alter_sql = self.ALTER_TABLE.format_map({
            'table': table.name,
            'params': self.simple_join(statements)
        })
        query = Query(self, [alter_sql], table=table)
        query.run(commit=True)

    def list_tables_sql(self):
        sql = self.SELECT.format(
            fields=self.comma_join(['rowid', 'name']),
            table='sqlite_schema'
        )
        not_like_clause = self.NOT_LIKE.format(
            field='name',
            wildcard=self.quote_value('sqlite_%')
        )
        where_clause = self.WHERE_CLAUSE.format(
            params=self.simple_join([
                self.EQUALITY.format(
                    field='type',
                    value=self.quote_value('table')
                ),
                self.AND.format(rhv=not_like_clause)
            ])
        )
        query = Query(self, [sql, where_clause])
        query.run()
        return query.result_cache


class BaseRow(Row):
    """Adds additional functionalities to
    the default SQLite `Row` class. Rows
    allows the data that comes from the database
    to be interfaced

    >>> row = table.get(name='Kendall')
    ... <BaseRow [{'rowid': 1}]>
    ... row['rowid']
    ... 1
    """

    backend_class = SQLiteBackend

    def __repr__(self):
        values = {}
        for key in self.keys():
            values[key] = self[key]
        return f'<{self.__class__.__name__} [{values}]>'

    def __contains__(self, value):
        return any((value in self[key] for key in self.keys))

    def __eq__(self, value):
        return any((self[key] == value for key in self.keys()))

    # def __setitem__(self, key, value):
    #     backend = self.initialize_backend
    #     update_sql = backend.UPDATE.format_map({
    #         'table': '',
    #         'params': backend.EQUALITY.format_map({
    #             'lhv': key,
    #             'rhv': backend.quote_value(value)
    #         })
    #     })
    #     where_clause = backend.WHERE_CLAUSE.format_map({
    #         'lhv': 'rowid',
    #         'rhv': self['id']
    #     })
    #     sql = [update_sql, where_clause]
    #     setattr(self, key, value)

    #     query = Query(backend, sql, table=None)
    #     query.run()
    #     return self

    @property
    def initialize_backend(self):
        return self.backend_class(database=DATABASE)

    # def delete(self):
    #     backend = self.initialize_backend
    #     delete_sql = backend.DELETE.format(table='')
    #     where_clause = backend.WHERE_CLAUSE.format_map({
    #         'params': backend.EQUALITY.format_map({
    #             'lhv': 'rowid',
    #             'rhv': self['rowid']
    #         })
    #     })
    #     sql = [delete_sql, where_clause]
    #     query = Query(backend, sql, table=None)
    #     query.run(commit=True)


class Migrations:
    """Main class to manage the 
    `migrations.json` file"""

    CACHE = {}
    backend_class = SQLiteBackend

    def __init__(self):
        self.file = PROJECT_PATH / 'migrations.json'
        self.CACHE = self.read_content
        self.file_id = self.CACHE['id']

        try:
            self.tables = self.CACHE['tables']
        except KeyError:
            raise KeyError('Migration file is not valid')
        self.migration_table_map = [table['name'] for table in self.tables]
        self.fields_map = defaultdict(list)

        self.tables_for_creation = set()
        self.tables_for_deletion = set()
        self.existing_tables = set()
        self.has_migrations = False

    def __repr__(self):
        return f'<{self.__class__.__name__} [{self.file_id}]>'

    @cached_property
    def read_content(self):
        with open(self.file, mode='r') as f:
            return json.load(f)

    def _write_fields(self, table):
        fields_map = []
        for name, field in table.fields_map.items():
            field_name, verbose_name, params = list(field.deconstruct())
            fields_map.append({
                'name': field_name,
                'verbose_name': verbose_name,
                'params': params
            })
        self.fields_map[table.name] = fields_map

    def _write_indexes(self, table):
        return []

    def create_migration_table(self):
        table_fields = [
            Field('name'),
            Field('applied')
        ]
        table = Table('migrations', '', fields=table_fields)
        table.prepare()

    def check(self, table_instances={}):
        errors = []
        for name, table_instance in table_instances.items():
            if not isinstance(table_instance, Table):
                errors.append(
                    f"Value should be instance "
                    f"of Table. Got: {table_instance}"
                )

        if errors:
            raise ValueError(*errors)

        backend = self.backend_class(database=DATABASE)
        database_tables = backend.list_tables_sql()
        # When the table is in the migration file
        # and not in the database, it needs to be
        # created
        for table_name in self.migration_table_map:
            table_exists = not any(
                map(lambda x: x['name'] == table_name, database_tables))
            if table_exists:
                self.tables_for_creation.add(table_name)

        # When the table is not in the migration
        # file but present in the database
        # it needs to be deleted
        for database_row in database_tables:
            if database_row['name'] not in self.migration_table_map:
                self.tables_for_deletion.add(database_row)

        sqls_to_run = []

        if self.tables_for_creation:
            for table_name in self.tables_for_creation:
                table = table_instances.get(table_name, None)
                if table is None:
                    continue

                table.prepare()
            self.has_migrations = True

        if self.tables_for_deletion:
            sql_script = []
            for database_row in self.tables_for_deletion:
                sql = self.backend_class.DROP_TABLE.format(
                    table=database_row['name']
                )
                sql_script.append(sql)
            sql = backend.build_script(*sql_script)
            sqls_to_run.append(sql)
            self.has_migrations = True

        # For existing tables, check that the
        # fields are the same and well set as
        # indicated in the migration file
        for database_row in database_tables:
            if (database_row['name'] in self.tables_for_creation or
                    database_row['name'] in self.tables_for_deletion):
                continue

            table_instance = table_instances.get(table_name, None)
            if table_instance is None:
                continue

            self.check_fields(table_instances[database_row['name']], backend)

        cached_results = list(Query.run_multiple(backend, sqls_to_run))
        # self.migrate(table_instances)

        self.tables_for_creation.clear()
        self.tables_for_deletion.clear()
        backend.connection.close()

    def check_fields(self, table, backend):
        """Checks the migration file for fields
        in relationship with the table"""
        database_table_columns = backend.list_table_columns_sql(table)

        columns_to_create = set()
        for field_name in table.fields_map.keys():
            if field_name not in database_table_columns:
                columns_to_create.add(field_name)

        # TODO: Drop columns that were dropped in the database

        backend.create_table_fields(table, columns_to_create)

    def migrate(self, tables):
        # Write to the migrations.json file only if
        # necessary e.g. dropped tables, changed fields
        if self.has_migrations:
            cache_copy = self.CACHE.copy()
            with open(PROJECT_PATH / 'migrations.json', mode='w+') as f:
                cache_copy['id'] = secrets.token_hex(5)
                cache_copy['date'] = str(datetime.datetime.now())
                cache_copy['number'] = self.CACHE['number'] + 1

                cache_copy['tables'] = []
                for key, table in tables.items():
                    self._write_fields(table)
                    cache_copy['tables'].append({
                        'name': table.name,
                        'fields': self.fields_map[table.name],
                        'indexes': self._write_indexes(table)
                    })
                json.dump(cache_copy, f, indent=4, ensure_ascii=False)

    def get_table_fields(self, name):
        table_index = self.table_map.index(name)
        return self.tables[table_index]['fields']

    def reconstruct_table_fields(self, table):
        reconstructed_fields = []
        fields = self.get_table_fields(table)
        for field in fields:
            instance = Field.create(
                field['name'],
                field['params'],
                verbose_name=field['verbose_name']
            )
            reconstructed_fields.append(instance)
        return reconstructed_fields


class Field:
    python_type = str
    base_validators = []

    def __init__(self, name, *, null=False, primary_key=False, default=None, unique=False, validators=[]):
        self.name = name
        self.null = null
        self.primary_key = primary_key
        self.default = default
        self.unique = unique
        self.table = None
        self.base_field_parameters = [self.field_type, 'not null']

    def __repr__(self):
        return f'<{self.__class__.__name__}[{self.name}]>'

    def __hash__(self):
        return hash((self.name))

    def __eq__(self, value):
        if isinstance(value, Field):
            return value.name == self.name
        return self.name == value

    @property
    def field_type(self):
        return 'text'

    @classmethod
    def create(cls, name, params, verbose_name=None):
        instance = cls(name)
        instance.base_field_parameters = params
        instance.verbose_name = verbose_name
        if 'null' in params:
            instance.null = True

        if 'primary key' in params:
            instance.primary_key = True
        instance.field_parameters()
        return instance

    def to_python(self, data):
        return self.python_type(data)

    def to_database(self, data):
        if not isinstance(data, self.python_type):
            raise ValueError(
                f'{data} should be an instance of {self.python_type}')
        return self.python_type(data)

    def field_parameters(self):
        """Adapat the python function parameters to the
        database field creation paramters

        >>> Field('visited', default=False)
        ... ['visited', 'text', 'not null', 'default', 0]
        """
        base_parameters = self.base_field_parameters.copy()
        if self.null:
            base_parameters.pop(base_parameters.index('not null'))
            base_parameters.append('null')

        if self.primary_key:
            base_parameters.append('primary key')

        if self.default is not None:
            database_value = self.to_database(self.default)
            value = self.table.backend.quote_value(database_value)
            base_parameters.extend(['default', value])

        if self.unique:
            base_parameters.append('unique')
            if 'not null' not in base_parameters and 'null' in base_parameters:
                base_parameters.index(
                    'not null', base_parameters.index('null'))

        base_parameters.insert(0, self.name)
        self.base_field_parameters = base_parameters
        return base_parameters

    def prepare(self, table):
        if not isinstance(table, Table):
            raise ValueError()
        self.table = table

    def deconstruct(self):
        return (self.name, None, self.field_parameters())


class CharField(Field):
    pass


class IntegerField(Field):
    python_type = int

    @property
    def field_type(self):
        return 'integer'


class JSONField(Field):
    python_type = dict

    def to_python(self, data):
        return json.loads(data)

    def to_database(self, data):
        return json.dumps(data, ensure_ascii=False, sort_keys=True)


class BooleanField(Field):
    truth_types = ['true', 't', 1, '1']
    false_types = ['false', 'f', 0, '0']

    def to_python(self, data):
        if data in self.truth_types:
            return True

        if data in self.false_types:
            return False

    def to_database(self, data):
        if isinstance(data, bool):
            if data == True:
                return 1
            return 0

        if isinstance(data, str):
            if data in self.truth_types:
                return 1

            if data in self.false_types:
                return 0
        return data


# class TableRegistry:
#     table_map = OrderedDict()
#     table_names = []
#     active_tables = set()

#     def __repr__(self):
#         return f'<{self.__class__.__name__} [{self.number_of_tables}]>'

#     @property
#     def number_of_tables(self):
#         return len(self.table_map.keys())

#     def add_table(self, name, table):
#         self.table_map[name] = table
#         self.table_names.append(name)

#     def table_exists(self, name):
#         return name in self.table_names

#     def set_active_tables(self, tables):
#         self.active_tables.update(tables)

#     def inactive_tables(self):
#         return set(self.active_tables).difference(self.table_names)


# registry = TableRegistry()


class Query:
    """This class represents an sql statement query
    and is responsible for executing the query on the
    database. The return data is stored on
    the `result_cache`
    """

    def __init__(self, backend, sql_tokens, table=None):
        self._table = table
        if not isinstance(backend, SQLiteBackend):
            raise ValueError('Connection should be an instance SQLiteBackend')

        self._backend = backend
        self._sql = None
        self._sql_tokens = sql_tokens
        self.result_cache = []

    def __repr__(self):
        return f'<{self.__class__.__name__} [{self._sql}]>'

    def __del__(self):
        self._backend.connection.close()

    @classmethod
    def run_multiple(cls, backend, *sqls, **kwargs):
        """Runs multiple queries against the database"""
        for sql in sqls:
            instance = cls(backend, sql, **kwargs)
            instance.run(commit=True)
            yield instance

    @classmethod
    def create(cls, backend, sql_tokens, table=None):
        """Creates a new `Query` class to be executed"""
        return cls(backend, sql_tokens, table=table)

    def prepare_sql(self):
        """Prepares a statement before it is sent
        to the database by joining the sql statements
        and implement a `;` to the end

        >>> ["select url from seen_urls", "where url='http://'"]
        ... "select url from seen_urls where url='http://';"
        """
        sql = self._backend.simple_join(self._sql_tokens)
        self._sql = self._backend.finalize_sql(sql)

    def run(self, commit=False):
        """Runs an sql statement and stores the
        return data on the `result_cache`"""
        self.prepare_sql()
        result = self._backend.connection.execute(self._sql)
        if commit:
            self._backend.connection.commit()
        self.result_cache = list(result)


# class ResultIterator:
#     def __init__(self):
#         self.query = None

#     def __get__(self, instance, cls=None):
#         self.query = instance.query
#         fields = ['rowid', 'name']
#         for result in self.query.run():
#             yield Row()

class QuerySet:
    # result_cache = ResultIterator()

    def __init__(self, query):
        if not isinstance(query, Query):
            raise ValueError()
        self.query = query
        self.result_cache = []

    def __str__(self):
        self.load_cache()
        return str(self.result_cache)

    def __iter__(self):
        self.load_cache()
        for item in self.result_cache:
            yield item

    def load_cache(self):
        if self.result_cache is None:
            self.result_cache = self.query.run()

    def exclude(self, **kwargs):
        pass

    def order_by(self, *fields):
        ascending_fields = set()
        descending_fields = set()
        for field in fields:
            if field.startswith('-'):
                field = field.removeprefix('-')
                descending_fields.add(field)
            else:
                ascending_fields.add(field)
        previous_sql = self.query._backend.de_sqlize_statement(self.query._sql)
        ascending_statements = [
            self.query._backend.ASCENDING.format_map({'field': field})
            for field in ascending_fields
        ]
        descending_statements = [
            self.query._backend.DESCENDNIG.format_map({'field': field})
            for field in descending_fields
        ]
        final_statement = ascending_statements + descending_statements
        order_by_clause = self.query._backend.ORDER_BY.format_map({
            'conditions': self.query._backend.comma_join(final_statement)
        })
        sql = [previous_sql, order_by_clause]
        new_query = self.query.create(
            self.query._backend, sql, table=self.query._table)
        new_query.run()
        # return QuerySet(new_query)
        return new_query.result_cache


class BaseTable(type):
    def __new__(cls, name, bases, attrs):
        super_new = super().__new__
        if 'prepare' in attrs:
            new_class = super_new(cls, name, bases, attrs)
            cls.prepare(new_class)
            return new_class
        return super_new(cls, name, bases, attrs)

    @classmethod
    def prepare(cls, table):
        pass


class AbstractTable(metaclass=BaseTable):
    query_class = Query
    backend_class = SQLiteBackend

    def __init__(self, database=None):
        self.backend = self.backend_class(database=database or DATABASE)
        # registry.add_table(self.name, self)

    def __hash__(self):
        return hash((self.name))

    def validate_values(self, fields, values):
        """Validate an incoming value in regards
        to the related field the user is trying
        to set on the column. The returned values
        are quoted by default"""
        validates_values = []
        for i, field in enumerate(fields):
            field = self.fields_map[field]
            validated_value = self.backend.quote_value(
                field.to_database(list(values)[i])
            )
            validates_values.append(validated_value)
        return validates_values

    def all(self):
        all_sql = self.backend.SELECT.format_map({
            'fields': self.backend.comma_join(['rowid', '*']),
            'table': self.name
        })
        sql = [all_sql]
        query = self.query_class(self.backend, sql, table=self)
        query.run()
        return query.result_cache
        # return QuerySet(query)

    def filter(self, **kwargs):
        tokens = self.backend.decompose_filters(**kwargs)
        return_type, filters = self.backend.build_filters(tokens)

        if len(filters) > 1:
            filters = [' and '.join(filters)]

        select_sql = self.backend.SELECT.format_map({
            'fields': self.backend.comma_join(['rowid', '*']),
            'table': self.name,
        })
        where_clause = self.backend.WHERE_CLAUSE.format_map({
            'params': self.backend.comma_join(filters)
        })
        sql = [select_sql, where_clause]
        query = self.query_class(self.backend, sql, table=self)
        query.run()
        return query.result_cache

    def first(self):
        result = self.all()
        return result[0]

    def last(self):
        result = self.all()
        return result[-1]

    def create(self, **kwargs):
        fields, values = self.backend.dict_to_sql(kwargs, quote_values=False)
        values = self.validate_values(fields, values)

        joined_fields = self.backend.comma_join(fields)
        joined_values = self.backend.comma_join(values)
        sql = self.backend.INSERT.format(
            table=self.name,
            fields=joined_fields,
            values=joined_values
        )
        query = self.query_class(self.backend, [sql])
        query.run(commit=True)
        return self.last()

    def get(self, **kwargs):
        base_return_fields = ['rowid', '*']
        filters = self.backend.build_filters(
            self.backend.decompose_filters(**kwargs)
        )

        # Functions SQL: select rowid, *, lower(url) from table
        select_sql = self.backend.SELECT.format_map({
            'fields': self.backend.comma_join(base_return_fields),
            'table': self.name
        })
        sql = [select_sql]

        # Filters SQL: select rowid, * from table where url='http://'
        joined_statements = ' and '.join(filters)
        where_clause = self.backend.WHERE_CLAUSE.format_map({
            'params': joined_statements
        })
        sql.extend([where_clause])

        query = self.query_class(self.backend, sql, table=self)
        query.run()

        if not query.result_cache:
            return None

        if len(query.result_cache) > 1:
            raise ValueError('Returned more than 1 value')

        return query.result_cache[0]

    def annotate(self, **kwargs):
        base_return_fields = ['rowid', '*']
        fields = self.backend.build_annotation(**kwargs)
        base_return_fields.extend(fields)

        sql = self.backend.SELECT.format_map({
            'fields': self.backend.comma_join(base_return_fields),
            'table': self.name
        })

        query = Query(self.backend, [sql], table=self)
        query.run()
        return query.result_cache


class Table(AbstractTable):
    """Represents a table in the database. This class
    can be used independently but would require creating
    and managing table creation

    To create a table without using `Database`:

    >>> table = Table('my_table', 'my_database', fields=[Field('url')])
    ... table.prepare()
    ... table.create(url='http://example.come')

    However, if you wish to manage a migration file and other table related
    tasks, wrapping tables in `Database` is the best option:

    >>> table = Table('my_table', 'my_database', fields=[Field('url')])
    ... database = Database('my_database', table)
    ... database.make_migrations()
    ... database.migrate()
    ... table.create(url='http://example.com')
    """
    fields_map = OrderedDict()

    def __init__(self, name, database, *, fields=[]):
        self.name = name
        self.query = None
        super().__init__(database=database)

        for field in fields:
            if not isinstance(field, Field):
                raise ValueError()

            field.prepare(self)
            self.fields_map[field.name] = field

    def __repr__(self):
        return f'<{self.__class__.__name__} [{self.name}]>'

    def has_field(self, name):
        return name in self.fields_map

    def create_table_sql(self, fields):
        sql = self.backend.CREATE_TABLE.format_map({
            'table': self.name,
            'fields': fields
        })
        return [sql]

    def drop_table_sql(self, name):
        sql = self.backend.DROP_TABLE.format_map({
            'table': name
        })
        return [sql]

    def build_field_parameters(self):
        return [
            field.field_parameters()
            for field in self.fields_map.values()
        ]

    def prepare(self):
        """Prepares a table for creation in 
        the database"""
        field_params = self.build_field_parameters()
        field_params = [
            self.backend.simple_join(params)
            for params in field_params
        ]
        sql = self.create_table_sql(self.backend.comma_join(field_params))
        query = self.query_class(self.backend, sql, table=self)
        self.query = query
        query.run(commit=True)


class Database:
    """This class links and unifies independent
    tables together and allows the management of
    a migration file

    Creating a new database can be done by doing the following steps:

    >>> table = Table('my_table', 'my_database', fields=[Field('url')])
    ... database = Database('my_database', table)
    ... database.make_migrations()
    ... database.migrate()
    ... table.create(url='http://example.com')

    Connections to the database are opened at the table level
    """

    migrations = Migrations()

    def __init__(self, name, *tables):
        self.table_map = {}
        for table in tables:
            if not isinstance(table, Table):
                raise ValueError('Value should be an instance of Table')
            self.table_map[table.name] = table

        self.database_name = name
        self.table_instances = list(tables)

    def __repr__(self):
        tables = list(self.table_map.values())
        return f'<{self.__class__.__name__} {tables}>'

    def __getitem__(self, table_name):
        return self.table_map[table_name]

    def get_table(self, table_name):
        return self.table_map[table_name]

    def make_migrations(self):
        """Updates the migration file with the
        local changes to the tables. Make migrations
        should generally be called before running `migrate`
        """
        self.migrations.has_migrations = True
        self.migrations.migrate(self.table_instances)

    def migrate(self):
        """Implements the changes to the migration
        file into the SQLite database"""
        self.migrations.check(self.table_instances)


table = Table('seen_urls', 'scraping', fields=[
    Field('url'),
    BooleanField('visited', default=False)
])
table.prepare()


# database = Database('seen_urls', table)
# database.make_migrations()
# database.migrate()

# def make_migrations(*tables):
#     """Writes the physical changes to the
#     tables to the `migrations.json` file"""
#     migrations = Migrations()
#     migrations.has_migrations = True
#     instances = {table.name: table}
#     migrations.migrate(instances)


# def migrate(*tables):
#     """Applies the migrations in the
#     `migrations.json` file to the database"""
#     migrations = Migrations()
#     instances = {table.name: table}
#     migrations.check(table_instances=instances)


# make_migrations()

# migrate()

# table.create(url='http://google.com', visited=True)
# r = table.filter(url__startswith='http')
# r = table.filter(url__contains='google')
# r = table.get(rowid=1)
# print(r)
# table.create(url='http://example.com')
# r = table.annotate(lowered_url=Lower('url'))
# print(r)

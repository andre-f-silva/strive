import sys

import ply.lex as lex
import ply.yacc as yacc

from domain.domain_model import *

# Token definitions
tokens = (
    'SYSTEM',
    'MICROSERVICE',
    'ENDPOINT',
    'TABLE',
    'COLUMN',
    'OPERATION',
    'NONCONFLICT',
    'ID',
    'INPUT',
    'OUTPUT',
    'TYPE',
    'TRANSACTIONS',
    'READ',
    'WRITE',
    'COMMA',
    'COLON',
    'LBRACE',
    'RBRACE',
    'LSQUARE',
    'RSQUARE'
)

# Regular expression rules for tokens
t_SYSTEM = r'System'
t_MICROSERVICE = r'Microservice'
t_ENDPOINT = r'Endpoint'
t_TABLE = r'Table'
t_COLUMN = r'Column'
t_OPERATION = r'Operation'
t_NONCONFLICT = r'nonConflict'
t_ID = r'[a-zA-Z_][a-zA-Z0-9_]*'
t_INPUT = r'input'
t_OUTPUT = r'output'
t_TYPE = r'type'
t_TRANSACTIONS = r'transactions'
t_READ = r'read'
t_WRITE = r'write'
t_COMMA = r','
t_COLON = r':'
t_LBRACE = r'{'
t_RBRACE = r'}'
t_LSQUARE = r'\['
t_RSQUARE = r'\]'

# Ignored characters
t_ignore = ' \t\n'

# Error handling rule
def t_error(t):
    print(f"Invalid token: {t.value[0]}", file=sys.stderr)
    t.lexer.skip(1)

# Parsing rules
def p_system(p):
    '''system : SYSTEM LBRACE microservice RBRACE'''
    p[0] = System([p[3]])

def p_microservice(p):
    '''microservice : MICROSERVICE ID LBRACE endpoint_table_operation_nonconflict RBRACE'''
    p[0] = Microservice(p[2], p[4][0], p[4][1], p[4][2])

def p_endpoint_table_operation_nonconflict(p):
    '''endpoint_table_operation_nonconflict : endpoint_table_operation_nonconflict endpoint
                                            | endpoint_table_operation_nonconflict table
                                            | endpoint_table_operation_nonconflict operation
                                            | endpoint_table_operation_nonconflict non_conflict
                                            | endpoint_table_operation_nonconflict COLUMN LSQUARE column_list RSQUARE
                                            | endpoint_table_operation_nonconflict TRANSACTIONS COLON LBRACE transaction_list RBRACE
                                            | empty'''

    if len(p) == 2:
        p[0] = ([], [], [])

    elif isinstance(p[2], Endpoint):
        p[0] = (p[1][0] + [p[2]], p[1][1], p[1][2])

    elif isinstance(p[2], Table):
        p[0] = (p[1][0] + [p[2]], p[1][1], p[1][2])

    elif isinstance(p[2], Operation):
        p[0] = (p[1][0], p[1][1] + [p[2]], p[1][2])

    elif isinstance(p[2], list):
        p[0] = (p[1][0], p[1][1], p[1][2] + p[2])

    elif isinstance(p[2], tuple):
        p[0] = (p[1][0], p[1][1], p[1][2])

    else:
        raise ValueError("error", p, file=sys.stderr)

def parse_system(input_str):
    lexer = lex.lex()
    parser = yacc.yacc()
    return parser.parse(input_str)


input_str = """
SYSTEM {
  MICROSERVICE client {
    ENDPOINT getScore {
      input: [userId]
      output: [score]
      transactions: [
        score = [score = readScoreOperation(userId)]
      ]
    }

    ENDPOINT updateScore {
      input: [userId, amount]
      output: [balance]
      transactions: [
        score = [score = writeScoreOperation(userId, amount)]
      ]
    }

    TABLE user {
      columns: [id, username, score]
    }

    OPERATION readScoreOperation(userId) {
      type: read
      table: user
    }

    OPERATION writeScoreOperation(userId, amount) {
      type: write
      table: user
    }

    nonConflict: []
  }
}
"""

system = parse_system(input_str)
print(system)
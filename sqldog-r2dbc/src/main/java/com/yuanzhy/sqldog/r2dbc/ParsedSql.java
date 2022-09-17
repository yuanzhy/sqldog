package com.yuanzhy.sqldog.r2dbc;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

class ParsedSql {

    private final String sql;

    private final List<Statement> statements;

    private final int statementCount;

    private final int parameterCount;

    public ParsedSql(String sql, List<Statement> statements) {
        this.sql = sql;
        this.statements = statements;
        this.statementCount = statements.size();
        this.parameterCount = getParameterCount(statements);
    }

    List<Statement> getStatements() {
        return this.statements;
    }

    public int getStatementCount() {
        return this.statementCount;
    }

    public int getParameterCount() {
        return this.parameterCount;
    }

    public String getSql() {
        return this.sql;
    }

    private static int getParameterCount(List<Statement> statements) {
        int sum = 0;
        for (Statement statement : statements) {
            sum += statement.getParameterCount();
        }
        return sum;
    }

    public boolean hasDefaultTokenValue(String... tokenValues) {
        for (Statement statement : this.statements) {
            for (Token token : statement.getTokens()) {
                if (token.getType() == TokenType.DEFAULT) {
                    for (String value : tokenValues) {
                        if (token.getValue().equalsIgnoreCase(value)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    static class Token {

        private final TokenType type;

        private final String value;

        public Token(TokenType type, String value) {
            this.type = type;
            this.value = value;
        }

        public TokenType getType() {
            return this.type;
        }

        public String getValue() {
            return this.value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Token token = (Token) o;

            if (this.type != token.type) {
                return false;
            }
            return this.value.equals(token.value);
        }

        @Override
        public int hashCode() {
            int result = this.type.hashCode();
            result = 31 * result + this.value.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Token{" +
                "type=" + this.type +
                ", value=" + this.value +
                '}';
        }

    }

    static class Statement {

        private final List<Token> tokens;

        private final int parameterCount;

        public Statement(List<Token> tokens) {
            this.tokens = tokens;
            this.parameterCount = readParameterCount(tokens);
        }

        public List<Token> getTokens() {
            return this.tokens;
        }

        public int getParameterCount() {
            return this.parameterCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Statement that = (Statement) o;

            return this.tokens.equals(that.tokens);
        }

        @Override
        public int hashCode() {
            return this.tokens.hashCode();
        }

        @Override
        public String toString() {
            return "Statement{" +
                "tokens=" + this.tokens +
                '}';
        }

        private static int readParameterCount(List<Token> tokens) {
            Set<Integer> parameters = new TreeSet<>();
            int questionMark = 0;
            for (Token token : tokens) {
                if (token.getType() != TokenType.PARAMETER) {
                    continue;
                }

                try {
                    if (token.getValue().startsWith("$")) {
                        int i = Integer.parseInt(token.getValue().substring(1));
                        parameters.add(i);
                    } else {
                        questionMark++;
                    }
                } catch (NumberFormatException | IndexOutOfBoundsException e) {
                    throw new IllegalArgumentException("Illegal parameter token: " + token.getValue());
                }
            }

            int current = 1;
            for (Integer i : parameters) {
                if (i != current) {
                    throw new IllegalArgumentException("Missing parameter number $" + i);
                }
                current++;
            }
            return parameters.size() + questionMark;
        }

    }

    enum TokenType {
        DEFAULT,
        STRING_CONSTANT,
        COMMENT,
        PARAMETER,
        QUOTED_IDENTIFIER,
        STATEMENT_END,
        SPECIAL_OR_OPERATOR
    }

}

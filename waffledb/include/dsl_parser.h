// waffledb/include/dsl_parser.h
#ifndef DSL_PARSER_H
#define DSL_PARSER_H

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <variant>
#include <chrono>

namespace waffledb
{

    // Forward declarations
    struct TimePoint;

    // AST nodes for query representation
    namespace ast
    {

        // Value types in queries
        using Value = std::variant<double, int64_t, std::string, bool>;

        // Base expression node
        struct Expression
        {
            virtual ~Expression() = default;
            virtual std::string toString() const = 0;
        };

        // Metric reference
        struct MetricRef : Expression
        {
            std::string name;
            std::unordered_map<std::string, std::string> tags;

            explicit MetricRef(const std::string &n) : name(n) {}
            std::string toString() const override;
        };

        // Time window specifications
        struct TimeWindow
        {
            enum Type
            {
                TUMBLING,
                SLIDING,
                SESSION
            };
            Type type;
            std::chrono::milliseconds duration;
            std::chrono::milliseconds slide; // For sliding windows

            TimeWindow(Type t, int64_t d, int64_t s = 0)
                : type(t), duration(d), slide(s) {}
        };

        // Aggregation functions
        struct AggregateFunc : Expression
        {
            enum Type
            {
                SUM,
                AVG,
                MIN,
                MAX,
                COUNT,
                RATE,
                DERIVATIVE
            };
            Type type;
            std::shared_ptr<Expression> expr;
            std::shared_ptr<TimeWindow> window;

            AggregateFunc(Type t, std::shared_ptr<Expression> e)
                : type(t), expr(e) {}
            std::string toString() const override;
        };

        // Binary operations
        struct BinaryOp : Expression
        {
            enum Type
            {
                ADD,
                SUB,
                MUL,
                DIV,
                AND,
                OR,
                EQ,
                NE,
                LT,
                LE,
                GT,
                GE
            };
            Type type;
            std::shared_ptr<Expression> left;
            std::shared_ptr<Expression> right;

            BinaryOp(Type t, std::shared_ptr<Expression> l, std::shared_ptr<Expression> r)
                : type(t), left(l), right(r) {}
            std::string toString() const override;
        };

        // Time range
        struct TimeRange : Expression
        {
            std::chrono::system_clock::time_point start;
            std::chrono::system_clock::time_point end;

            TimeRange(const std::chrono::system_clock::time_point &s,
                      const std::chrono::system_clock::time_point &e)
                : start(s), end(e) {}
            std::string toString() const override;
        };

        // Query statement
        struct Query
        {
            std::vector<std::shared_ptr<Expression>> select;
            std::shared_ptr<MetricRef> from;
            std::shared_ptr<Expression> where;
            std::shared_ptr<TimeRange> timeRange;
            std::vector<std::string> groupBy;
            std::shared_ptr<TimeWindow> window;

            std::string toString() const;
        };

    } // namespace ast

    // Lexer tokens
    enum class TokenType
    {
        // Literals
        NUMBER,
        STRING,
        IDENTIFIER,
        TIMESTAMP,

        // Keywords
        SELECT,
        FROM,
        WHERE,
        GROUP,
        BY,
        WINDOW,
        SUM,
        AVG,
        MIN,
        MAX,
        COUNT,
        RATE,
        DERIVATIVE,
        TUMBLING,
        SLIDING,
        SESSION,
        AND,
        OR,
        NOT,

        // Operators
        PLUS,
        MINUS,
        STAR,
        SLASH,
        EQ,
        NE,
        LT,
        LE,
        GT,
        GE,

        // Delimiters
        LPAREN,
        RPAREN,
        LBRACKET,
        RBRACKET,
        LBRACE,
        RBRACE,
        COMMA,
        DOT,
        COLON,
        SEMICOLON,

        // Special
        EOF_TOKEN,
        ERROR
    };

    struct Token
    {
        TokenType type;
        std::string value;
        size_t line;
        size_t column;
    };

    // Lexer for tokenizing queries
    class Lexer
    {
    private:
        std::string input_;
        size_t position_ = 0;
        size_t line_ = 1;
        size_t column_ = 1;

    public:
        explicit Lexer(const std::string &input);

        Token nextToken();
        std::vector<Token> tokenize();

    private:
        char peek() const;
        char advance();
        void skipWhitespace();
        Token readNumber();
        Token readIdentifier();
        Token readString();
        Token readTimestamp();
    };

    // Parser for building AST from tokens
    class Parser
    {
    private:
        std::vector<Token> tokens_;
        size_t current_ = 0;

    public:
        explicit Parser(const std::vector<Token> &tokens);

        std::shared_ptr<ast::Query> parse();

        // Error information
        struct ParseError
        {
            std::string message;
            size_t line;
            size_t column;
        };

        const std::vector<ParseError> &getErrors() const { return errors_; }

    private:
        std::vector<ParseError> errors_;

        // Parsing methods
        std::shared_ptr<ast::Query> parseQuery();
        std::vector<std::shared_ptr<ast::Expression>> parseSelectList();
        std::shared_ptr<ast::Expression> parseExpression();
        std::shared_ptr<ast::Expression> parsePrimary();
        std::shared_ptr<ast::Expression> parseBinary(int precedence);
        std::shared_ptr<ast::MetricRef> parseMetricRef();
        std::shared_ptr<ast::TimeRange> parseTimeRange();
        std::shared_ptr<ast::TimeWindow> parseWindow();
        std::shared_ptr<ast::AggregateFunc> parseAggregate();

        // Helper methods
        bool match(TokenType type);
        bool check(TokenType type) const;
        Token advance();
        Token peek() const;
        Token previous() const;
        bool isAtEnd() const;
        void error(const std::string &message);

        // Operator precedence
        int getPrecedence(TokenType type) const;
    };

    // Query executor
    class QueryExecutor
    {
    private:
        class TimeSeriesDatabase *db_;

    public:
        explicit QueryExecutor(TimeSeriesDatabase *database);

        std::vector<TimePoint> execute(const std::shared_ptr<ast::Query> &query);

        // Temporal aggregation results
        struct AggregateResult
        {
            uint64_t timestamp;
            double value;
            std::string metric;
            std::unordered_map<std::string, std::string> tags;
        };

        std::vector<AggregateResult> executeAggregate(
            const std::shared_ptr<ast::Query> &query);

    private:
        // Execution helpers
        std::vector<TimePoint> executeSimpleQuery(
            const std::shared_ptr<ast::Query> &query);
        std::vector<AggregateResult> executeWindowedAggregate(
            const std::shared_ptr<ast::Query> &query);

        double evaluateAggregate(
            ast::AggregateFunc::Type type,
            const std::vector<TimePoint> &points);
    };

    // Main DSL interface
    class QueryDSL
    {
    private:
        std::unique_ptr<QueryExecutor> executor_;

    public:
        explicit QueryDSL(TimeSeriesDatabase *database);
        ~QueryDSL();

        // Parse and execute query
        std::vector<TimePoint> query(const std::string &dsl);

        // Validate query without executing
        bool validate(const std::string &dsl, std::vector<std::string> &errors);

        // Get query plan (for debugging)
        std::string explain(const std::string &dsl);
    };

} // namespace waffledb

#endif // DSL_PARSER_H
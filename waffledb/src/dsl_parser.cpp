// waffledb/src/dsl_parser.cpp
#include "dsl_parser.h"
#include "waffledb.h"
#include <sstream>
#include <iomanip>
#include <cctype>
#include <algorithm>

namespace waffledb
{

    // AST implementations
    namespace ast
    {

        std::string MetricRef::toString() const
        {
            std::string result = name;
            if (!tags.empty())
            {
                result += "{";
                bool first = true;
                for (const auto &[key, value] : tags)
                {
                    if (!first)
                        result += ", ";
                    result += key + "=\"" + value + "\"";
                    first = false;
                }
                result += "}";
            }
            return result;
        }

        std::string AggregateFunc::toString() const
        {
            std::string funcName;
            switch (type)
            {
            case SUM:
                funcName = "sum";
                break;
            case AVG:
                funcName = "avg";
                break;
            case MIN:
                funcName = "min";
                break;
            case MAX:
                funcName = "max";
                break;
            case COUNT:
                funcName = "count";
                break;
            case RATE:
                funcName = "rate";
                break;
            case DERIVATIVE:
                funcName = "derivative";
                break;
            }

            std::string result = funcName + "(" + expr->toString() + ")";
            if (window)
            {
                result += " over ";
                switch (window->type)
                {
                case TimeWindow::TUMBLING:
                    result += "tumbling";
                    break;
                case TimeWindow::SLIDING:
                    result += "sliding";
                    break;
                case TimeWindow::SESSION:
                    result += "session";
                    break;
                }
                result += " " + std::to_string(window->duration.count()) + "ms";
                if (window->slide.count() > 0)
                {
                    result += " slide " + std::to_string(window->slide.count()) + "ms";
                }
            }
            return result;
        }

        std::string BinaryOp::toString() const
        {
            std::string op;
            switch (type)
            {
            case ADD:
                op = "+";
                break;
            case SUB:
                op = "-";
                break;
            case MUL:
                op = "*";
                break;
            case DIV:
                op = "/";
                break;
            case AND:
                op = "and";
                break;
            case OR:
                op = "or";
                break;
            case EQ:
                op = "=";
                break;
            case NE:
                op = "!=";
                break;
            case LT:
                op = "<";
                break;
            case LE:
                op = "<=";
                break;
            case GT:
                op = ">";
                break;
            case GE:
                op = ">=";
                break;
            }
            return "(" + left->toString() + " " + op + " " + right->toString() + ")";
        }

        std::string TimeRange::toString() const
        {
            auto startTime = std::chrono::system_clock::to_time_t(start);
            auto endTime = std::chrono::system_clock::to_time_t(end);

            std::stringstream ss;
            struct tm startTm = {}, endTm = {};
#if defined(_WIN32) || defined(_WIN64)
            localtime_s(&startTm, &startTime);
            localtime_s(&endTm, &endTime);
#else
            localtime_r(&startTime, &startTm);
            localtime_r(&endTime, &endTm);
#endif

            ss << "between " << std::put_time(&startTm, "%Y-%m-%d %H:%M:%S")
               << " and " << std::put_time(&endTm, "%Y-%m-%d %H:%M:%S");
            return ss.str();
        }

        std::string Query::toString() const
        {
            std::stringstream ss;

            ss << "SELECT ";
            for (size_t i = 0; i < select.size(); ++i)
            {
                if (i > 0)
                    ss << ", ";
                ss << select[i]->toString();
            }

            if (from)
            {
                ss << " FROM " << from->toString();
            }

            if (where)
            {
                ss << " WHERE " << where->toString();
            }

            if (timeRange)
            {
                ss << " " << timeRange->toString();
            }

            if (!groupBy.empty())
            {
                ss << " GROUP BY ";
                for (size_t i = 0; i < groupBy.size(); ++i)
                {
                    if (i > 0)
                        ss << ", ";
                    ss << groupBy[i];
                }
            }

            if (window)
            {
                ss << " WINDOW ";
                switch (window->type)
                {
                case TimeWindow::TUMBLING:
                    ss << "TUMBLING";
                    break;
                case TimeWindow::SLIDING:
                    ss << "SLIDING";
                    break;
                case TimeWindow::SESSION:
                    ss << "SESSION";
                    break;
                }
                ss << " " << window->duration.count() << "ms";
                if (window->slide.count() > 0)
                {
                    ss << " SLIDE " << window->slide.count() << "ms";
                }
            }

            return ss.str();
        }

    } // namespace ast

    // Lexer implementation
    Lexer::Lexer(const std::string &input) : input_(input) {}

    char Lexer::peek() const
    {
        if (position_ >= input_.length())
        {
            return '\0';
        }
        return input_[position_];
    }

    char Lexer::advance()
    {
        if (position_ >= input_.length())
        {
            return '\0';
        }
        char c = input_[position_++];
        if (c == '\n')
        {
            line_++;
            column_ = 1;
        }
        else
        {
            column_++;
        }
        return c;
    }

    void Lexer::skipWhitespace()
    {
        while (std::isspace(peek()))
        {
            advance();
        }
    }

    Token Lexer::readNumber()
    {
        size_t startPos = position_;
        size_t startCol = column_;

        while (std::isdigit(peek()) || peek() == '.')
        {
            advance();
        }

        return {TokenType::NUMBER, input_.substr(startPos, position_ - startPos), line_, startCol};
    }

    Token Lexer::readIdentifier()
    {
        size_t startPos = position_;
        size_t startCol = column_;

        while (std::isalnum(peek()) || peek() == '_' || peek() == '.')
        {
            advance();
        }

        std::string value = input_.substr(startPos, position_ - startPos);

        // Convert to lowercase for keyword comparison
        std::string lowerValue = value;
        std::transform(lowerValue.begin(), lowerValue.end(), lowerValue.begin(),
                       [](char c)
                       { return static_cast<char>(std::tolower(static_cast<unsigned char>(c))); });

        // Check for keywords (now case-insensitive)
        static const std::unordered_map<std::string, TokenType> keywords = {
            {"select", TokenType::SELECT},
            {"from", TokenType::FROM},
            {"where", TokenType::WHERE},
            {"group", TokenType::GROUP},
            {"by", TokenType::BY},
            {"window", TokenType::WINDOW},
            {"sum", TokenType::SUM},
            {"avg", TokenType::AVG},
            {"min", TokenType::MIN},
            {"max", TokenType::MAX},
            {"count", TokenType::COUNT},
            {"rate", TokenType::RATE},
            {"derivative", TokenType::DERIVATIVE},
            {"tumbling", TokenType::TUMBLING},
            {"sliding", TokenType::SLIDING},
            {"session", TokenType::SESSION},
            {"and", TokenType::AND},
            {"or", TokenType::OR},
            {"not", TokenType::NOT}};

        auto it = keywords.find(lowerValue);
        if (it != keywords.end())
        {
            return {it->second, value, line_, startCol}; // Return original case but use lowercase for lookup
        }

        return {TokenType::IDENTIFIER, value, line_, startCol};
    }

    Token Lexer::readString()
    {
        size_t startCol = column_;
        advance(); // Skip opening quote

        size_t startPos = position_;
        while (peek() != '"' && peek() != '\0')
        {
            if (peek() == '\\')
            {
                advance(); // Skip escape character
            }
            advance();
        }

        if (peek() == '\0')
        {
            return {TokenType::ERROR, "Unterminated string", line_, startCol};
        }

        std::string value = input_.substr(startPos, position_ - startPos);
        advance(); // Skip closing quote

        return {TokenType::STRING, value, line_, startCol};
    }

    Token Lexer::nextToken()
    {
        skipWhitespace();

        if (position_ >= input_.length())
        {
            return {TokenType::EOF_TOKEN, "", line_, column_};
        }

        char c = peek();

        // Numbers
        if (std::isdigit(c))
        {
            return readNumber();
        }

        // Identifiers and keywords
        if (std::isalpha(c) || c == '_')
        {
            return readIdentifier();
        }

        // Strings
        if (c == '"')
        {
            return readString();
        }

        // Single-character tokens
        size_t col = column_;
        advance();

        switch (c)
        {
        case '+':
            return {TokenType::PLUS, "+", line_, col};
        case '-':
            return {TokenType::MINUS, "-", line_, col};
        case '*':
            return {TokenType::STAR, "*", line_, col};
        case '/':
            return {TokenType::SLASH, "/", line_, col};
        case '(':
            return {TokenType::LPAREN, "(", line_, col};
        case ')':
            return {TokenType::RPAREN, ")", line_, col};
        case '{':
            return {TokenType::LBRACE, "{", line_, col};
        case '}':
            return {TokenType::RBRACE, "}", line_, col};
        case ',':
            return {TokenType::COMMA, ",", line_, col};
        case '.':
            return {TokenType::DOT, ".", line_, col};
        case ':':
            return {TokenType::COLON, ":", line_, col};
        case ';':
            return {TokenType::SEMICOLON, ";", line_, col};
        case '=':
        {
            if (peek() == '=')
            {
                advance();
                return {TokenType::EQ, "==", line_, col};
            }
            return {TokenType::EQ, "=", line_, col};
        }
        case '!':
        {
            if (peek() == '=')
            {
                advance();
                return {TokenType::NE, "!=", line_, col};
            }
            return {TokenType::ERROR, "!", line_, col};
        }
        case '<':
        {
            if (peek() == '=')
            {
                advance();
                return {TokenType::LE, "<=", line_, col};
            }
            return {TokenType::LT, "<", line_, col};
        }
        case '>':
        {
            if (peek() == '=')
            {
                advance();
                return {TokenType::GE, ">=", line_, col};
            }
            return {TokenType::GT, ">", line_, col};
        }
        default:
            return {TokenType::ERROR, std::string(1, c), line_, col};
        }
    }

    std::vector<Token> Lexer::tokenize()
    {
        std::vector<Token> tokens;
        Token token;

        do
        {
            token = nextToken();
            tokens.push_back(token);
        } while (token.type != TokenType::EOF_TOKEN && token.type != TokenType::ERROR);

        return tokens;
    }

    // Parser implementation
    Parser::Parser(const std::vector<Token> &tokens) : tokens_(tokens) {}

    bool Parser::match(TokenType type)
    {
        if (check(type))
        {
            advance();
            return true;
        }
        return false;
    }

    bool Parser::check(TokenType type) const
    {
        if (isAtEnd())
            return false;
        return peek().type == type;
    }

    Token Parser::advance()
    {
        if (!isAtEnd())
            current_++;
        return previous();
    }

    Token Parser::peek() const
    {
        return tokens_[current_];
    }

    Token Parser::previous() const
    {
        return tokens_[current_ - 1];
    }

    bool Parser::isAtEnd() const
    {
        return peek().type == TokenType::EOF_TOKEN;
    }

    void Parser::error(const std::string &message)
    {
        Token token = peek();
        errors_.push_back({message, token.line, token.column});
    }

    std::shared_ptr<ast::Query> Parser::parse()
    {
        return parseQuery();
    }

    std::shared_ptr<ast::Query> Parser::parseQuery()
    {
        auto query = std::make_shared<ast::Query>();

        // SELECT clause
        if (!match(TokenType::SELECT))
        {
            error("Expected SELECT");
            return nullptr;
        }

        query->select = parseSelectList();

        // FROM clause
        if (match(TokenType::FROM))
        {
            query->from = parseMetricRef();
        }

        // WHERE clause (simplified for now)
        if (match(TokenType::WHERE))
        {
            // Parse time range for now
            query->timeRange = parseTimeRange();
        }

        // WINDOW clause
        if (match(TokenType::WINDOW))
        {
            query->window = parseWindow();
        }

        return query;
    }

    std::vector<std::shared_ptr<ast::Expression>> Parser::parseSelectList()
    {
        std::vector<std::shared_ptr<ast::Expression>> expressions;

        do
        {
            if (check(TokenType::SUM) || check(TokenType::AVG) ||
                check(TokenType::MIN) || check(TokenType::MAX) ||
                check(TokenType::COUNT) || check(TokenType::RATE) ||
                check(TokenType::DERIVATIVE))
            {
                expressions.push_back(parseAggregate());
            }
            else
            {
                expressions.push_back(parseExpression());
            }
        } while (match(TokenType::COMMA));

        return expressions;
    }

    std::shared_ptr<ast::Expression> Parser::parseExpression()
    {
        return parseBinary(0);
    }

    std::shared_ptr<ast::Expression> Parser::parsePrimary()
    {
        if (check(TokenType::IDENTIFIER))
        {
            return parseMetricRef();
        }

        if (match(TokenType::LPAREN))
        {
            auto expr = parseExpression();
            if (!match(TokenType::RPAREN))
            {
                error("Expected ')'");
            }
            return expr;
        }

        error("Expected expression");
        return nullptr;
    }

    std::shared_ptr<ast::MetricRef> Parser::parseMetricRef()
    {
        if (!check(TokenType::IDENTIFIER))
        {
            error("Expected metric name");
            return nullptr;
        }

        auto metric = std::make_shared<ast::MetricRef>(advance().value);

        // Parse tags if present
        if (match(TokenType::LBRACE))
        {
            do
            {
                if (!check(TokenType::IDENTIFIER))
                {
                    error("Expected tag key");
                    break;
                }
                std::string key = advance().value;

                if (!match(TokenType::EQ))
                {
                    error("Expected '='");
                    break;
                }

                if (!check(TokenType::STRING))
                {
                    error("Expected tag value");
                    break;
                }
                std::string value = advance().value;

                metric->tags[key] = value;
            } while (match(TokenType::COMMA));

            if (!match(TokenType::RBRACE))
            {
                error("Expected '}'");
            }
        }

        return metric;
    }

    std::shared_ptr<ast::AggregateFunc> Parser::parseAggregate()
    {
        ast::AggregateFunc::Type type;

        switch (peek().type)
        {
        case TokenType::SUM:
            type = ast::AggregateFunc::SUM;
            break;
        case TokenType::AVG:
            type = ast::AggregateFunc::AVG;
            break;
        case TokenType::MIN:
            type = ast::AggregateFunc::MIN;
            break;
        case TokenType::MAX:
            type = ast::AggregateFunc::MAX;
            break;
        case TokenType::COUNT:
            type = ast::AggregateFunc::COUNT;
            break;
        case TokenType::RATE:
            type = ast::AggregateFunc::RATE;
            break;
        case TokenType::DERIVATIVE:
            type = ast::AggregateFunc::DERIVATIVE;
            break;
        default:
            error("Expected aggregate function");
            return nullptr;
        }

        advance();

        if (!match(TokenType::LPAREN))
        {
            error("Expected '('");
            return nullptr;
        }

        auto expr = parseExpression();

        if (!match(TokenType::RPAREN))
        {
            error("Expected ')'");
            return nullptr;
        }

        return std::make_shared<ast::AggregateFunc>(type, expr);
    }

    std::shared_ptr<ast::TimeRange> Parser::parseTimeRange()
    {
        // Simplified time range parsing
        // In a real implementation, would parse timestamp expressions

        auto now = std::chrono::system_clock::now();
        auto start = now - std::chrono::hours(1); // Default: last hour
        auto end = now;

        return std::make_shared<ast::TimeRange>(start, end);
    }

    std::shared_ptr<ast::TimeWindow> Parser::parseWindow()
    {
        ast::TimeWindow::Type type = ast::TimeWindow::TUMBLING;

        if (match(TokenType::TUMBLING))
        {
            type = ast::TimeWindow::TUMBLING;
        }
        else if (match(TokenType::SLIDING))
        {
            type = ast::TimeWindow::SLIDING;
        }
        else if (match(TokenType::SESSION))
        {
            type = ast::TimeWindow::SESSION;
        }

        if (!check(TokenType::NUMBER))
        {
            error("Expected window duration");
            return nullptr;
        }

        int64_t duration = std::stoll(advance().value);
        int64_t slide = 0;

        // Parse slide duration for sliding windows
        if (type == ast::TimeWindow::SLIDING && match(TokenType::IDENTIFIER))
        {
            if (previous().value == "slide" && check(TokenType::NUMBER))
            {
                slide = std::stoll(advance().value);
            }
        }

        return std::make_shared<ast::TimeWindow>(type, duration, slide);
    }

    std::shared_ptr<ast::Expression> Parser::parseBinary(int precedence)
    {
        auto left = parsePrimary();

        while (true)
        {
            TokenType op = peek().type;
            int prec = getPrecedence(op);

            if (prec < precedence)
            {
                break;
            }

            advance();

            ast::BinaryOp::Type opType;
            switch (op)
            {
            case TokenType::PLUS:
                opType = ast::BinaryOp::ADD;
                break;
            case TokenType::MINUS:
                opType = ast::BinaryOp::SUB;
                break;
            case TokenType::STAR:
                opType = ast::BinaryOp::MUL;
                break;
            case TokenType::SLASH:
                opType = ast::BinaryOp::DIV;
                break;
            case TokenType::AND:
                opType = ast::BinaryOp::AND;
                break;
            case TokenType::OR:
                opType = ast::BinaryOp::OR;
                break;
            case TokenType::EQ:
                opType = ast::BinaryOp::EQ;
                break;
            case TokenType::NE:
                opType = ast::BinaryOp::NE;
                break;
            case TokenType::LT:
                opType = ast::BinaryOp::LT;
                break;
            case TokenType::LE:
                opType = ast::BinaryOp::LE;
                break;
            case TokenType::GT:
                opType = ast::BinaryOp::GT;
                break;
            case TokenType::GE:
                opType = ast::BinaryOp::GE;
                break;
            default:
                error("Invalid operator");
                return left;
            }

            auto right = parseBinary(prec + 1);
            left = std::make_shared<ast::BinaryOp>(opType, left, right);
        }

        return left;
    }

    int Parser::getPrecedence(TokenType type) const
    {
        switch (type)
        {
        case TokenType::OR:
            return 1;
        case TokenType::AND:
            return 2;
        case TokenType::EQ:
        case TokenType::NE:
            return 3;
        case TokenType::LT:
        case TokenType::LE:
        case TokenType::GT:
        case TokenType::GE:
            return 4;
        case TokenType::PLUS:
        case TokenType::MINUS:
            return 5;
        case TokenType::STAR:
        case TokenType::SLASH:
            return 6;
        default:
            return 0;
        }
    }

    // QueryExecutor implementation
    QueryExecutor::QueryExecutor(TimeSeriesDatabase *database) : db_(database) {}

    std::vector<TimePoint> QueryExecutor::execute(const std::shared_ptr<ast::Query> &query)
    {
        if (!query->window)
        {
            return executeSimpleQuery(query);
        }
        else
        {
            // Convert windowed aggregate results to TimePoints
            auto results = executeWindowedAggregate(query);
            std::vector<TimePoint> points;

            for (const auto &result : results)
            {
                TimePoint point;
                point.timestamp = result.timestamp;
                point.value = result.value;
                point.metric = result.metric;
                point.tags = result.tags;
                points.push_back(point);
            }

            return points;
        }
    }

    std::vector<TimePoint> QueryExecutor::executeSimpleQuery(
        const std::shared_ptr<ast::Query> &query)
    {

        if (!query->from || !query->timeRange)
        {
            return {};
        }

        uint64_t startTime = std::chrono::duration_cast<std::chrono::seconds>(
                                 query->timeRange->start.time_since_epoch())
                                 .count();
        uint64_t endTime = std::chrono::duration_cast<std::chrono::seconds>(
                               query->timeRange->end.time_since_epoch())
                               .count();

        return db_->query(query->from->name, startTime, endTime, query->from->tags);
    }

    std::vector<QueryExecutor::AggregateResult> QueryExecutor::executeWindowedAggregate(
        const std::shared_ptr<ast::Query> &query)
    {

        std::vector<AggregateResult> results;

        if (!query->from || !query->timeRange || !query->window)
        {
            return results;
        }

        // Get raw data
        uint64_t startTime = std::chrono::duration_cast<std::chrono::seconds>(
                                 query->timeRange->start.time_since_epoch())
                                 .count();
        uint64_t endTime = std::chrono::duration_cast<std::chrono::seconds>(
                               query->timeRange->end.time_since_epoch())
                               .count();

        auto points = db_->query(query->from->name, startTime, endTime, query->from->tags);

        if (points.empty())
        {
            return results;
        }

        // Apply windowing
        uint64_t windowDuration = query->window->duration.count() / 1000; // Convert to seconds
        uint64_t slideInterval = query->window->slide.count() / 1000;

        if (slideInterval == 0)
        {
            slideInterval = windowDuration; // Tumbling window
        }

        // Process windows
        for (uint64_t windowStart = startTime; windowStart < endTime; windowStart += slideInterval)
        {
            uint64_t windowEnd = windowStart + windowDuration;

            // Collect points in this window
            std::vector<TimePoint> windowPoints;
            for (const auto &point : points)
            {
                if (point.timestamp >= windowStart && point.timestamp < windowEnd)
                {
                    windowPoints.push_back(point);
                }
            }

            if (!windowPoints.empty())
            {
                // Calculate aggregate for this window
                AggregateResult result;
                result.timestamp = windowStart;
                result.metric = query->from->name;
                result.tags = query->from->tags;

                // Assume first select expression is an aggregate
                if (!query->select.empty())
                {
                    if (auto aggFunc = std::dynamic_pointer_cast<ast::AggregateFunc>(query->select[0]))
                    {
                        result.value = evaluateAggregate(aggFunc->type, windowPoints);
                    }
                }

                results.push_back(result);
            }
        }

        return results;
    }

    double QueryExecutor::evaluateAggregate(
        ast::AggregateFunc::Type type,
        const std::vector<TimePoint> &points)
    {

        if (points.empty())
        {
            return 0.0;
        }

        switch (type)
        {
        case ast::AggregateFunc::SUM:
        {
            double sum = 0.0;
            for (const auto &point : points)
            {
                sum += point.value;
            }
            return sum;
        }

        case ast::AggregateFunc::AVG:
        {
            double sum = 0.0;
            for (const auto &point : points)
            {
                sum += point.value;
            }
            return sum / points.size();
        }

        case ast::AggregateFunc::MIN:
        {
            double min = points[0].value;
            for (const auto &point : points)
            {
                min = std::min(min, point.value);
            }
            return min;
        }

        case ast::AggregateFunc::MAX:
        {
            double max = points[0].value;
            for (const auto &point : points)
            {
                max = std::max(max, point.value);
            }
            return max;
        }

        case ast::AggregateFunc::COUNT:
        {
            return static_cast<double>(points.size());
        }

        case ast::AggregateFunc::RATE:
        {
            if (points.size() < 2)
                return 0.0;

            double valueDiff = points.back().value - points.front().value;
            double timeDiff = static_cast<double>(points.back().timestamp - points.front().timestamp);

            return timeDiff > 0 ? valueDiff / timeDiff : 0.0;
        }

        case ast::AggregateFunc::DERIVATIVE:
        {
            if (points.size() < 2)
                return 0.0;

            const auto &p1 = points[points.size() - 2];
            const auto &p2 = points[points.size() - 1];

            double valueDiff = p2.value - p1.value;
            double timeDiff = static_cast<double>(p2.timestamp - p1.timestamp);

            return timeDiff > 0 ? valueDiff / timeDiff : 0.0;
        }

        default:
            return 0.0;
        }
    }

    // QueryDSL implementation
    QueryDSL::QueryDSL(TimeSeriesDatabase *database)
        : executor_(std::make_unique<QueryExecutor>(database)) {}

    QueryDSL::~QueryDSL() = default;

    std::vector<TimePoint> QueryDSL::query(const std::string &dsl)
    {
        Lexer lexer(dsl);
        auto tokens = lexer.tokenize();

        Parser parser(tokens);
        auto ast = parser.parse();

        if (!ast || !parser.getErrors().empty())
        {
            // Handle parse errors
            return {};
        }

        return executor_->execute(ast);
    }

    bool QueryDSL::validate(const std::string &dsl, std::vector<std::string> &errors)
    {
        Lexer lexer(dsl);
        auto tokens = lexer.tokenize();

        Parser parser(tokens);
        auto ast = parser.parse();

        if (!parser.getErrors().empty())
        {
            for (const auto &error : parser.getErrors())
            {
                errors.push_back(error.message + " at line " + std::to_string(error.line) +
                                 ", column " + std::to_string(error.column));
            }
            return false;
        }

        return ast != nullptr;
    }

    std::string QueryDSL::explain(const std::string &dsl)
    {
        Lexer lexer(dsl);
        auto tokens = lexer.tokenize();

        Parser parser(tokens);
        auto ast = parser.parse();

        if (!ast)
        {
            return "Parse error";
        }

        return ast->toString();
    }

} // namespace waffledb
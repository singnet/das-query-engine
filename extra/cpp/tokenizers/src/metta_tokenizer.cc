#include <stdexcept>
#include <tuple>
#include <vector>

#include "metta_tokenizer.h"

using namespace std;

// -------------------------------------------------------------------------------------------------
/**
 * @brief Parses a MeTTa expression into a tokenized string stream.
 *
 * This function processes the input MeTTa expression string starting from the given cursor position
 * and returns a pair containing the updated cursor position and the tokenized string stream.
 *
 * @param expression The input MeTTa expression string to be tokenized.
 * @param cursor The starting position in the expression string. Defaults to 0.
 * @return A pair containing the updated cursor position and the tokenized string stream.
 * @throws runtime_error if the expression is invalid.
 */
pair<size_t, string> _tokenize(const string& expression, size_t cursor = 0) {
    string output;
    string header = "LINK Expression";
    int target_count = 0;
    string token;
    char ch;
    size_t start = cursor;

    for (; cursor < expression.size(); cursor++) {
        ch = expression[cursor];

        if (ch == '(') {
            if (cursor > start) {
                tie(cursor, token) = _tokenize(expression, cursor);
                output += " " + token;
                target_count++;
            }
            continue;

        } else if (ch == ')') {
            return make_pair(cursor, header + " " + to_string(target_count) + output);

        } else if (isspace(ch)) {
            continue;

        } else {
            token.clear();
            while (
                cursor < expression.size()
                and not isspace(expression[cursor])
                and expression[cursor] != '('
                and expression[cursor] != ')'
            ) {
                token += expression[cursor++];
            }
            --cursor;

            if (token[0] == '$') {
                header = "LINK_TEMPLATE Expression";
                output += " VARIABLE " + token.substr(1);
                target_count++;
            } else {
                output += " NODE Symbol " + token;
                target_count++;
            }
        }
    }

    throw runtime_error("Invalid expression");
}

// -------------------------------------------------------------------------------------------------
string tokenize(const string& expression) {
    auto [_, tokenized_query] = _tokenize(expression);
    return tokenized_query;
}

// -------------------------------------------------------------------------------------------------


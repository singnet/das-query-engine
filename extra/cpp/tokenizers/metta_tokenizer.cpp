#include <iostream>
#include <string>
#include <vector>
#include <tuple>

using namespace std;


/**
 * @brief Parses a MeTTa query into a tokenized string stream.
 *
 * This function processes the input MeTTa query string starting from the given cursor position
 * and returns a pair containing the updated cursor position and the tokenized string stream.
 *
 * @param query The input MeTTa query string to be parsed.
 * @param cursor The starting position in the query string. Defaults to 0.
 * @return A pair containing the updated cursor position and the tokenized string stream.
 * @throws runtime_error if the query is invalid.
 */
pair<size_t, string> _parse_query(const string& query, size_t cursor = 0) {
    string output;
    string link_header = "LINK Expression ";
    int target_count = 0;
    string token;
    char ch;
    size_t start = cursor;

    for (; cursor < query.size(); cursor++) {
        ch = query[cursor];

        if (ch == '(') {
            if (cursor > start) {
                tie(cursor, token) = _parse_query(query, cursor);
                output += token;
                target_count++;
            }
            continue;

        } else if (ch == ')') {
            return make_pair(cursor, link_header + to_string(target_count) + " " + output);

        } else if (isspace(ch)) {
            continue;

        } else {
            token = "";
            while (
                cursor < query.size()
                and not isspace(query[cursor])
                and query[cursor] != '('
                and query[cursor] != ')'
            ) {
                token += query[cursor++];
            }
            --cursor;

            if (token[0] == '$') {
                link_header = "LINK_TEMPLATE Expression ";
                output += "VARIABLE " + token.substr(1) + " ";
                target_count++;
            } else {
                output += "NODE Symbol " + token + " ";
                target_count++;
            }
        }
    }

    throw runtime_error("Invalid query");
}

/**
 * @brief Parses a MeTTa query into a tokenized string stream.
 *
 * This function processes the input MeTTa query string and converts it into a tokenized string
 * stream. The query is expected to be in the format `(Similarity (Concept "human") $v1)`, where
 * elements inside the parentheses are links of type `Expression`. Each element inside the
 * parentheses, such as `Similarity`, `Concept`, and `"human"`, are nodes of type `Symbol`,
 * except for those that start with `$`, which are variables.
 *
 * Example:
 * 
 * Input: `(Similarity (Concept "human") $v1)`
 * 
 * Output: `LINK_TEMPLATE Expression 3 NODE Symbol Similarity LINK Expression 2 NODE Symbol Concept NODE Symbol "human" VARIABLE v1`
 *
 * @param query The input MeTTa query string to be parsed.
 * @return A tokenized string stream representing the parsed query.
 * @throws runtime_error if the query is invalid.
 */
string parse_query(const string& query) {
    auto [_, tokenized_query] = _parse_query(query);
    return tokenized_query;
}

int main() {
    string query = "(Similarity (Concept \"human\") $v1)";
    auto tokenized_query = parse_query(query);
    cout << tokenized_query << endl;
    return 0;
}
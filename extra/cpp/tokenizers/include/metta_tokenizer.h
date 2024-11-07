#pragma once

#include <string>

using namespace std;

// -------------------------------------------------------------------------------------------------
/**
 * @brief Parses a MeTTa expression into a tokenized string stream.
 *
 * This function processes the input MeTTa expression string and converts it into a tokenized string
 * stream. The expression is expected to be in the format `(Similarity (Concept "human") $v1)`, where
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
 * @param expression The input MeTTa expression string to be tokenized.
 * @return A tokenized string stream representing the parsed expression.
 * @throws runtime_error if the expression is invalid.
 */
string tokenize(const string& expression);

// -------------------------------------------------------------------------------------------------


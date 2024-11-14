#include <iostream>

#include "metta_tokenizer.h"

using namespace std;

// -------------------------------------------------------------------------------------------------
int main() {
    string expression = "(Similarity (Concept \"human\") $v1)";
    auto tokenized = tokenize(expression);
    cout << tokenize(expression) << endl;
    return 0;
}

// -------------------------------------------------------------------------------------------------

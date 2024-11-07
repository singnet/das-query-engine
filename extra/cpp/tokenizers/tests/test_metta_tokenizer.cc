#include <gtest/gtest.h>

#include "metta_tokenizer.h"

TEST(MettaTokenizerTest, BasicAssertions) {
    string expected = "LINK_TEMPLATE Expression 3 NODE Symbol Similarity LINK Expression 2 NODE Symbol Concept NODE Symbol \"human\" VARIABLE v1";
    string expression = "(Similarity (Concept \"human\") $v1)";
    string actual = tokenize(expression);
    actual.erase(actual.find_last_not_of(' ') + 1);  // trim trailing spaces
    EXPECT_EQ(actual, expected);

    expected = "LINK_TEMPLATE Expression 4 NODE Symbol Similarity VARIABLE v0 LINK Expression 2 NODE Symbol Concept NODE Symbol \"human\" VARIABLE v1";
    expression = "(Similarity $v0 (Concept \"human\") $v1)";
    actual = tokenize(expression);
    actual.erase(actual.find_last_not_of(' ') + 1);  // trim trailing spaces
    EXPECT_EQ(actual, expected);

    expected = "LINK_TEMPLATE Expression 3 NODE Symbol Similarity LINK_TEMPLATE Expression 2 NODE Symbol Concept VARIABLE v0 VARIABLE v1";
    expression = "(Similarity (Concept $v0) $v1)";
    actual = tokenize(expression);
    actual.erase(actual.find_last_not_of(' ') + 1);  // trim trailing spaces
    EXPECT_EQ(actual, expected);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

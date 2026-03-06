import ballerina/test;

// Test resources for repeat directive tests
json[] repeatTestResources = [
    {
        "resourceType": "QuestionnaireResponse",
        "id": "qr1",
        "item": [
            {
                "linkId": "1",
                "text": "Group 1",
                "item": [
                    {
                        "linkId": "1.1",
                        "text": "Question 1.1",
                        "answer": [
                            {
                                "valueString": "Answer 1.1",
                                "item": [
                                    {
                                        "linkId": "1.1.1",
                                        "text": "Follow-up to 1.1"
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "linkId": "1.2",
                        "text": "Question 1.2",
                        "item": [
                            {
                                "linkId": "1.2.1",
                                "text": "Question 1.2.1"
                            }
                        ]
                    }
                ]
            },
            {
                "linkId": "2",
                "text": "Group 2"
            }
        ]
    }
];

@test:Config
function testRepeatBasic() returns error? {
    json viewDefinition = {
        "resource": "QuestionnaireResponse",
        "status": "active",
        "select": [
            {
                "column": [
                    {
                        "name": "id",
                        "path": "id",
                        "type": "id"
                    }
                ]
            },
            {
                "repeat": ["item"],
                "column": [
                    {
                        "name": "linkId",
                        "path": "linkId",
                        "type": "string"
                    },
                    {
                        "name": "text",
                        "path": "text",
                        "type": "string"
                    }
                ]
            }
        ]
    };

    json[] result = check evaluate(repeatTestResources, viewDefinition);

    json[] expected = [
        {
            "id": "qr1",
            "linkId": "1",
            "text": "Group 1"
        },
        {
            "id": "qr1",
            "linkId": "1.1",
            "text": "Question 1.1"
        },
        {
            "id": "qr1",
            "linkId": "1.2",
            "text": "Question 1.2"
        },
        {
            "id": "qr1",
            "linkId": "1.2.1",
            "text": "Question 1.2.1"
        },
        {
            "id": "qr1",
            "linkId": "2",
            "text": "Group 2"
        }
    ];

    test:assertEquals(result, expected, msg = "repeat: basic - should recursively traverse item elements");
}

@test:Config
function testRepeatItemAndAnswerItem() returns error? {
    json viewDefinition = {
        "resource": "QuestionnaireResponse",
        "status": "active",
        "select": [
            {
                "column": [
                    {
                        "name": "id",
                        "path": "id",
                        "type": "id"
                    }
                ]
            },
            {
                "repeat": ["item", "answer.item"],
                "column": [
                    {
                        "name": "linkId",
                        "path": "linkId",
                        "type": "string"
                    },
                    {
                        "name": "text",
                        "path": "text",
                        "type": "string"
                    }
                ]
            }
        ]
    };

    json[] result = check evaluate(repeatTestResources, viewDefinition);

    json[] expected = [
        {
            "id": "qr1",
            "linkId": "1",
            "text": "Group 1"
        },
        {
            "id": "qr1",
            "linkId": "1.1",
            "text": "Question 1.1"
        },
        {
            "id": "qr1",
            "linkId": "1.1.1",
            "text": "Follow-up to 1.1"
        },
        {
            "id": "qr1",
            "linkId": "1.2",
            "text": "Question 1.2"
        },
        {
            "id": "qr1",
            "linkId": "1.2.1",
            "text": "Question 1.2.1"
        },
        {
            "id": "qr1",
            "linkId": "2",
            "text": "Group 2"
        }
    ];

    test:assertEquals(result, expected, msg = "repeat: item and answer.item - should recursively traverse both item and answer.item elements");
}

@test:Config
function testRepeatEmptyExpression() returns error? {
    json viewDefinition = {
        "resource": "QuestionnaireResponse",
        "status": "active",
        "select": [
            {
                "column": [
                    {
                        "name": "id",
                        "path": "id",
                        "type": "id"
                    }
                ]
            },
            {
                "repeat": ["jurisdiction"],
                "column": [
                    {
                        "name": "code",
                        "path": "coding.code",
                        "type": "code"
                    }
                ]
            }
        ]
    };

    json[] result = check evaluate(repeatTestResources, viewDefinition);

    json[] expected = [];

    test:assertEquals(result, expected, msg = "repeat: empty expression - should return empty array when path doesn't exist");
}

@test:Config
function testRepeatEmptyChildExpression() returns error? {
    json viewDefinition = {
        "resource": "QuestionnaireResponse",
        "status": "active",
        "select": [
            {
                "column": [
                    {
                        "name": "id",
                        "path": "id",
                        "type": "id"
                    }
                ]
            },
            {
                "repeat": ["item"],
                "column": [
                    {
                        "name": "linkId",
                        "path": "linkId",
                        "type": "string"
                    },
                    {
                        "name": "definition",
                        "path": "definition",
                        "type": "uri"
                    }
                ]
            }
        ]
    };

    json[] result = check evaluate(repeatTestResources, viewDefinition);

    json[] expected = [
        {
            "id": "qr1",
            "linkId": "1",
            "definition": null
        },
        {
            "id": "qr1",
            "linkId": "1.1",
            "definition": null
        },
        {
            "id": "qr1",
            "linkId": "1.2",
            "definition": null
        },
        {
            "id": "qr1",
            "linkId": "1.2.1",
            "definition": null
        },
        {
            "id": "qr1",
            "linkId": "2",
            "definition": null
        }
    ];

    test:assertEquals(result, expected, msg = "repeat: empty child expression - should include null values for missing child elements");
}

@test:Config
function testRepeatCombinedWithForEach() returns error? {
    json viewDefinition = {
        "resource": "QuestionnaireResponse",
        "status": "active",
        "select": [
            {
                "column": [
                    {
                        "name": "id",
                        "path": "id",
                        "type": "id"
                    }
                ]
            },
            {
                "repeat": ["item"],
                "select": [
                    {
                        "column": [
                            {
                                "name": "linkId",
                                "path": "linkId",
                                "type": "string"
                            }
                        ]
                    },
                    {
                        "forEach": "answer",
                        "column": [
                            {
                                "name": "answerValue",
                                "path": "value.ofType(string)",
                                "type": "string"
                            }
                        ]
                    }
                ]
            }
        ]
    };

    json[] result = check evaluate(repeatTestResources, viewDefinition);

    json[] expected = [
        {
            "id": "qr1",
            "linkId": "1.1",
            "answerValue": "Answer 1.1"
        }
    ];

    test:assertEquals(result, expected, msg = "repeat: combined with forEach - should work together correctly");
}

@test:Config
function testRepeatCombinedWithForEachOrNull() returns error? {
    json viewDefinition = {
        "resource": "QuestionnaireResponse",
        "status": "active",
        "select": [
            {
                "column": [
                    {
                        "name": "id",
                        "path": "id",
                        "type": "id"
                    }
                ]
            },
            {
                "repeat": ["item"],
                "select": [
                    {
                        "column": [
                            {
                                "name": "linkId",
                                "path": "linkId",
                                "type": "string"
                            }
                        ]
                    },
                    {
                        "forEachOrNull": "answer",
                        "column": [
                            {
                                "name": "answerValue",
                                "path": "value.ofType(string)",
                                "type": "string"
                            }
                        ]
                    }
                ]
            }
        ]
    };

    json[] result = check evaluate(repeatTestResources, viewDefinition);

    json[] expected = [
        {
            "id": "qr1",
            "linkId": "1",
            "answerValue": null
        },
        {
            "id": "qr1",
            "linkId": "1.1",
            "answerValue": "Answer 1.1"
        },
        {
            "id": "qr1",
            "linkId": "1.2",
            "answerValue": null
        },
        {
            "id": "qr1",
            "linkId": "1.2.1",
            "answerValue": null
        },
        {
            "id": "qr1",
            "linkId": "2",
            "answerValue": null
        }
    ];

    test:assertEquals(result, expected, msg = "repeat: combined with forEachOrNull - should include null values for items without answers");
}

@test:Config
function testRepeatCombinedWithUnionAll() returns error? {
    json viewDefinition = {
        "resource": "QuestionnaireResponse",
        "status": "active",
        "select": [
            {
                "column": [
                    {
                        "name": "id",
                        "path": "id",
                        "type": "id"
                    }
                ]
            },
            {
                "unionAll": [
                    {
                        "repeat": ["item"],
                        "column": [
                            {
                                "name": "type",
                                "path": "'item'",
                                "type": "string"
                            },
                            {
                                "name": "linkId",
                                "path": "linkId",
                                "type": "string"
                            },
                            {
                                "name": "text",
                                "path": "text",
                                "type": "string"
                            }
                        ]
                    },
                    {
                        "repeat": ["item", "answer.item"],
                        "column": [
                            {
                                "name": "type",
                                "path": "'answer-item'",
                                "type": "string"
                            },
                            {
                                "name": "linkId",
                                "path": "linkId",
                                "type": "string"
                            },
                            {
                                "name": "text",
                                "path": "text",
                                "type": "string"
                            }
                        ]
                    }
                ]
            }
        ]
    };

    json[] result = check evaluate(repeatTestResources, viewDefinition);

    json[] expected = [
        {
            "id": "qr1",
            "type": "item",
            "linkId": "1",
            "text": "Group 1"
        },
        {
            "id": "qr1",
            "type": "item",
            "linkId": "1.1",
            "text": "Question 1.1"
        },
        {
            "id": "qr1",
            "type": "item",
            "linkId": "1.2",
            "text": "Question 1.2"
        },
        {
            "id": "qr1",
            "type": "item",
            "linkId": "1.2.1",
            "text": "Question 1.2.1"
        },
        {
            "id": "qr1",
            "type": "item",
            "linkId": "2",
            "text": "Group 2"
        },
        {
            "id": "qr1",
            "type": "answer-item",
            "linkId": "1",
            "text": "Group 1"
        },
        {
            "id": "qr1",
            "type": "answer-item",
            "linkId": "1.1",
            "text": "Question 1.1"
        },
        {
            "id": "qr1",
            "type": "answer-item",
            "linkId": "1.1.1",
            "text": "Follow-up to 1.1"
        },
        {
            "id": "qr1",
            "type": "answer-item",
            "linkId": "1.2",
            "text": "Question 1.2"
        },
        {
            "id": "qr1",
            "type": "answer-item",
            "linkId": "1.2.1",
            "text": "Question 1.2.1"
        },
        {
            "id": "qr1",
            "type": "answer-item",
            "linkId": "2",
            "text": "Group 2"
        }
    ];

    test:assertEquals(result, expected, msg = "repeat: combined with unionAll - should merge results from different repeat paths");
}

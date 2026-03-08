import ballerina/test;

// Test resources for validation tests
json[] validationTestResources = [
    {
        "resourceType": "Patient",
        "id": "pt1",
        "name": [
            {
                "given": ["John"],
                "family": "Doe"
            }
        ],
        "telecom": [
            {
                "value": "123-456-7890",
                "system": "phone"
            }
        ]
    }
];

// Tests for "Column Already Defined" validation error
@test:Config
function testValidationDuplicateColumnInSameSelect() returns error? {
    json viewDefinition = {
        "resource": "Patient",
        "status": "active",
        "select": [
            {
                "column": [
                    {
                        "name": "patient_id",
                        "path": "id",
                        "type": "id"
                    },
                    {
                        "name": "patient_id",
                        "path": "id",
                        "type": "id"
                    }
                ]
            }
        ]
    };

    json[]|error result = evaluate(validationTestResources, viewDefinition);

    test:assertTrue(result is error, msg = "Should return error for duplicate column name in same select");
    if result is error {
        test:assertTrue(result.message().includes("Column Already Defined"),
                msg = "Error message should indicate Column Already Defined");
    }
}

@test:Config
function testValidationDuplicateColumnAcrossSelects() returns error? {
    json viewDefinition = {
        "resource": "Patient",
        "status": "active",
        "select": [
            {
                "column": [
                    {
                        "name": "patient_id",
                        "path": "id",
                        "type": "id"
                    }
                ]
            },
            {
                "column": [
                    {
                        "name": "patient_id",
                        "path": "id",
                        "type": "id"
                    }
                ]
            }
        ]
    };

    json[]|error result = evaluate(validationTestResources, viewDefinition);

    test:assertTrue(result is error, msg = "Should return error for duplicate column name across selects");
    if result is error {
        test:assertTrue(result.message().includes("Column Already Defined"),
                msg = "Error message should indicate Column Already Defined");
    }
}

@test:Config
function testValidationDuplicateColumnInNestedForEach() returns error? {
    json viewDefinition = {
        "resource": "Patient",
        "status": "active",
        "select": [
            {
                "column": [
                    {
                        "name": "phone",
                        "path": "id",
                        "type": "id"
                    }
                ]
            },
            {
                "forEach": "telecom",
                "column": [
                    {
                        "name": "phone",
                        "path": "value",
                        "type": "string"
                    }
                ]
            }
        ]
    };

    json[]|error result = evaluate(validationTestResources, viewDefinition);

    test:assertTrue(result is error, msg = "Should return error for duplicate column name in nested forEach");
    if result is error {
        test:assertTrue(result.message().includes("Column Already Defined"),
                msg = "Error message should indicate Column Already Defined");
    }
}

// Tests for "Union Branches Inconsistent" validation error
@test:Config
function testValidationUnionBranchesColumnNameMismatch() returns error? {
    json viewDefinition = {
        "resource": "Patient",
        "status": "active",
        "select": [
            {
                "unionAll": [
                    {
                        "column": [
                            {
                                "name": "col_a",
                                "path": "id",
                                "type": "id"
                            }
                        ]
                    },
                    {
                        "column": [
                            {
                                "name": "col_b",
                                "path": "id",
                                "type": "id"
                            }
                        ]
                    }
                ]
            }
        ]
    };

    json[]|error result = evaluate(validationTestResources, viewDefinition);

    test:assertTrue(result is error, msg = "Should return error when union branches have different column names");
    if result is error {
        test:assertTrue(result.message().includes("Union Branches Inconsistent"),
                msg = "Error message should indicate Union Branches Inconsistent");
    }
}

@test:Config
function testValidationUnionBranchesColumnCountMismatch() returns error? {
    json viewDefinition = {
        "resource": "Patient",
        "status": "active",
        "select": [
            {
                "unionAll": [
                    {
                        "column": [
                            {
                                "name": "col_a",
                                "path": "id",
                                "type": "id"
                            },
                            {
                                "name": "col_b",
                                "path": "id",
                                "type": "id"
                            }
                        ]
                    },
                    {
                        "column": [
                            {
                                "name": "col_a",
                                "path": "id",
                                "type": "id"
                            }
                        ]
                    }
                ]
            }
        ]
    };

    json[]|error result = evaluate(validationTestResources, viewDefinition);

    test:assertTrue(result is error, msg = "Should return error when union branches have different column counts");
    if result is error {
        test:assertTrue(result.message().includes("Union Branches Inconsistent"),
                msg = "Error message should indicate Union Branches Inconsistent");
    }
}

@test:Config
function testValidationUnionBranchesColumnOrderMismatch() returns error? {
    json viewDefinition = {
        "resource": "Patient",
        "status": "active",
        "select": [
            {
                "unionAll": [
                    {
                        "column": [
                            {
                                "name": "col_a",
                                "path": "id",
                                "type": "id"
                            },
                            {
                                "name": "col_b",
                                "path": "id",
                                "type": "id"
                            }
                        ]
                    },
                    {
                        "column": [
                            {
                                "name": "col_b",
                                "path": "id",
                                "type": "id"
                            },
                            {
                                "name": "col_a",
                                "path": "id",
                                "type": "id"
                            }
                        ]
                    }
                ]
            }
        ]
    };

    json[]|error result = evaluate(validationTestResources, viewDefinition);

    test:assertTrue(result is error, msg = "Should return error when union branches have different column order");
    if result is error {
        test:assertTrue(result.message().includes("Union Branches Inconsistent"),
                msg = "Error message should indicate Union Branches Inconsistent");
    }
}

@test:Config
function testValidationUnionBranchesThreeWayMismatch() returns error? {
    json viewDefinition = {
        "resource": "Patient",
        "status": "active",
        "select": [
            {
                "unionAll": [
                    {
                        "column": [
                            {
                                "name": "col_a",
                                "path": "id",
                                "type": "id"
                            }
                        ]
                    },
                    {
                        "column": [
                            {
                                "name": "col_a",
                                "path": "id",
                                "type": "id"
                            }
                        ]
                    },
                    {
                        "column": [
                            {
                                "name": "col_b",
                                "path": "id",
                                "type": "id"
                            }
                        ]
                    }
                ]
            }
        ]
    };

    json[]|error result = evaluate(validationTestResources, viewDefinition);

    test:assertTrue(result is error, msg = "Should return error when third union branch differs");
    if result is error {
        test:assertTrue(result.message().includes("Union Branches Inconsistent"),
                msg = "Error message should indicate Union Branches Inconsistent");
    }
}

// Tests for valid view definitions (should not error)
@test:Config
function testValidationValidSimpleSelect() returns error? {
    json viewDefinition = {
        "resource": "Patient",
        "status": "active",
        "select": [
            {
                "column": [
                    {
                        "name": "id",
                        "path": "id",
                        "type": "id"
                    },
                    {
                        "name": "resource_type",
                        "path": "resourceType",
                        "type": "string"
                    }
                ]
            }
        ]
    };

    json[]|error result = evaluate(validationTestResources, viewDefinition);

    test:assertTrue(result is json[], msg = "Valid simple select should not return error");
}

@test:Config
function testValidationValidUnionAll() returns error? {
    json viewDefinition = {
        "resource": "Patient",
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
                        "forEach": "telecom",
                        "column": [
                            {
                                "name": "contact_value",
                                "path": "value",
                                "type": "string"
                            },
                            {
                                "name": "contact_system",
                                "path": "system",
                                "type": "code"
                            }
                        ]
                    },
                    {
                        "forEach": "name",
                        "column": [
                            {
                                "name": "contact_value",
                                "path": "family",
                                "type": "string"
                            },
                            {
                                "name": "contact_system",
                                "path": "use",
                                "type": "code"
                            }
                        ]
                    }
                ]
            }
        ]
    };

    json[]|error result = evaluate(validationTestResources, viewDefinition);

    test:assertTrue(result is json[], msg = "Valid unionAll with matching columns should not return error");
}

@test:Config
function testValidationValidNestedForEach() returns error? {
    json viewDefinition = {
        "resource": "Patient",
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
                "forEach": "telecom",
                "column": [
                    {
                        "name": "phone",
                        "path": "value",
                        "type": "string"
                    }
                ]
            }
        ]
    };

    json[]|error result = evaluate(validationTestResources, viewDefinition);

    test:assertTrue(result is json[], msg = "Valid nested forEach should not return error");
}

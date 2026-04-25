import ballerina/test;

// Test resources for where tests
json[] whereTestResources = [
    {
        "resourceType": "Patient",
        "id": "p1",
        "name": [
            {
                "use": "official",
                "family": "f1"
            }
        ]
    },
    {
        "resourceType": "Patient",
        "id": "p2",
        "name": [
            {
                "use": "nickname",
                "family": "f2"
            }
        ]
    },
    {
        "resourceType": "Patient",
        "id": "p3",
        "name": [
            {
                "use": "nickname",
                "given": ["g3"],
                "family": "f3"
            }
        ]
    },
    {
        "resourceType": "Observation",
        "id": "o1",
        "valueInteger": 12
    },
    {
        "resourceType": "Observation",
        "id": "o2",
        "valueInteger": 10
    }
];

@test:Config
function testWhereSimplePathWithResult() returns error? {
    json viewDefinition = {
        "resource": "Patient",
        "select": [
            {
                "column": [
                    {
                        "path": "id",
                        "name": "id",
                        "type": "id"
                    }
                ]
            }
        ],
        "where": [
            {
                "path": "name.where(use = 'official').exists()"
            }
        ]
    };

    json[] result = check evaluate(whereTestResources, viewDefinition);

    json[] expected = [
        {
            "id": "p1"
        }
    ];

    test:assertEquals(result, expected, msg = "Should return only patient with official name use");
}

@test:Config
function testWherePathWithNoResults() returns error? {
    json viewDefinition = {
        "resource": "Patient",
        "select": [
            {
                "column": [
                    {
                        "path": "id",
                        "name": "id",
                        "type": "id"
                    }
                ]
            }
        ],
        "where": [
            {
                "path": "name.where(use = 'maiden').exists()"
            }
        ]
    };

    json[] result = check evaluate(whereTestResources, viewDefinition);

    json[] expected = [];

    test:assertEquals(result, expected, msg = "Should return empty result when no patient has maiden name use");
}

@test:Config
function testWhereMultiplePaths() returns error? {
    json viewDefinition = {
        "resource": "Patient",
        "select": [
            {
                "column": [
                    {
                        "path": "id",
                        "name": "id",
                        "type": "id"
                    }
                ]
            }
        ],
        "where": [
            {
                "path": "name.where(use = 'official').exists()"
            },
            {
                "path": "name.where(family = 'f1').exists()"
            }
        ]
    };

    json[] result = check evaluate(whereTestResources, viewDefinition);

    json[] expected = [
        {
            "id": "p1"
        }
    ];

    test:assertEquals(result, expected, msg = "Should return patient matching both where conditions");
}

@test:Config
function testWherePathWithAndConnector() returns error? {
    json viewDefinition = {
        "resource": "Patient",
        "select": [
            {
                "column": [
                    {
                        "path": "id",
                        "name": "id",
                        "type": "id"
                    }
                ]
            }
        ],
        "where": [
            {
                "path": "name.where(use = 'official' and family = 'f1').exists()"
            }
        ]
    };

    json[] result = check evaluate(whereTestResources, viewDefinition);

    json[] expected = [
        {
            "id": "p1"
        }
    ];

    test:assertEquals(result, expected, msg = "Should return patient with name matching both use and family");
}

@test:Config
function testWherePathWithOrConnector() returns error? {
    json viewDefinition = {
        "resource": "Patient",
        "select": [
            {
                "column": [
                    {
                        "path": "id",
                        "name": "id",
                        "type": "id"
                    }
                ]
            }
        ],
        "where": [
            {
                "path": "name.where(use = 'official' or family = 'f2').exists()"
            }
        ]
    };

    json[] result = check evaluate(whereTestResources, viewDefinition);

    json[] expected = [
        {
            "id": "p1"
        },
        {
            "id": "p2"
        }
    ];

    test:assertEquals(result, expected, msg = "Should return patients with name matching use or family");
}

@test:Config
function testWherePathEvaluatesToTrueWhenEmpty() returns error? {
    json viewDefinition = {
        "resource": "Patient",
        "select": [
            {
                "column": [
                    {
                        "path": "id",
                        "name": "id",
                        "type": "id"
                    }
                ]
            }
        ],
        "where": [
            {
                "path": "name.where(family = 'f2').empty()"
            }
        ]
    };

    json[] result = check evaluate(whereTestResources, viewDefinition);

    json[] expected = [
        {
            "id": "p1"
        },
        {
            "id": "p3"
        }
    ];

    test:assertEquals(result, expected, msg = "Should return patients where name with family f2 is empty");
}

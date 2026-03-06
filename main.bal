import ballerina/io;

public function main() returns error? {
    json[] forEachResources = [
        {
            "resourceType": "Patient",
            "id": "pt1",
            "name": [
                {
                    "family": "F1.1"
                },
                {
                    "family": "F1.2"
                }
            ],
            "contact": [
                {
                    "telecom": [
                        {
                            "system": "phone"
                        }
                    ],
                    "name": {
                        "family": "FC1.1",
                        "given": ["N1", "N1`"]
                    }
                },
                {
                    "telecom": [
                        {
                            "system": "email"
                        }
                    ],
                    "gender": "unknown",
                    "name": {
                        "family": "FC1.2",
                        "given": ["N2"]
                    }
                }
            ]
        },
        {
            "resourceType": "Patient",
            "id": "pt2",
            "name": [
                {
                    "family": "F2.1"
                },
                {
                    "family": "F2.2"
                }
            ]
        },
        {
            "resourceType": "Patient",
            "id": "pt3"
        }
    ];
    json viewDefinition1 = {
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
                "forEach": "name",
                "column": [
                    {
                        "name": "family",
                        "path": "family",
                        "type": "string"
                    }
                ]
            }
        ]
    };

    json[] result = check evaluate(forEachResources, viewDefinition1);
    io:println(result);

}

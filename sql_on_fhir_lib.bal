import ballerinax/health.fhir.r4utils.fhirpath;

// Column type for validation
type ColumnDefinition record {
    string name;
};

// Validates the view definition columns and returns the list of column names
// C: Context columns (already defined columns)
// S: Selection structure to validate
isolated function validateColumns(json S, ColumnDefinition[] C) returns ColumnDefinition[]|error {
    // Initialize Ret to equal C
    ColumnDefinition[] Ret = C.clone();
    map<anydata> selection = check S.cloneWithType();

    // For each Column col in S.column[]
    if selection.hasKey("column") {
        json[] columns = check selection["column"].cloneWithType();
        foreach var col in columns {
            map<anydata> column = check col.cloneWithType();
            string colName = <string>column["name"];

            // If a Column with name col.name already exists in Ret, throw "Column Already Defined"
            foreach var existingCol in Ret {
                if existingCol.name == colName {
                    return error("Column Already Defined: " + colName);
                }
            }
            // Otherwise, append col to Ret
            Ret.push({name: colName});
        }
    }

    // For each Selection Structure sel in S.select[]
    if selection.hasKey("select") {
        json[] selects = check selection["select"].cloneWithType();
        foreach var sel in selects {
            // For each Column c in Validate(sel, Ret)
            ColumnDefinition[] validatedCols = check validateColumns(sel, Ret);
            // Get only the new columns (those not in Ret)
            foreach var c in validatedCols {
                boolean exists = false;
                foreach var existingCol in Ret {
                    if existingCol.name == c.name {
                        exists = true;
                        break;
                    }
                }
                if !exists {
                    Ret.push(c);
                }
            }
        }
    }

    // If S.unionAll[] is present
    if selection.hasKey("unionAll") {
        json[] unionAlls = check selection["unionAll"].cloneWithType();

        if unionAlls.length() > 0 {
            // Define u0 as Validate(S.unionAll[0], Ret)
            ColumnDefinition[] u0 = check validateColumns(unionAlls[0], Ret);
            // Get only new columns from u0 (not in Ret)
            string[] u0Names = [];
            foreach var col in u0 {
                boolean existsInRet = false;
                foreach var existingCol in Ret {
                    if existingCol.name == col.name {
                        existsInRet = true;
                        break;
                    }
                }
                if !existsInRet {
                    u0Names.push(col.name);
                }
            }

            // For each Selection Structure sel in S.unionAll[] (starting from index 1)
            foreach int i in 1 ..< unionAlls.length() {
                // Define u as ValidateColumns(sel, Ret)
                ColumnDefinition[] u = check validateColumns(unionAlls[i], Ret);

                // Get only new column names from u (not in Ret)
                string[] uNames = [];
                foreach var col in u {
                    boolean existsInRet = false;
                    foreach var existingCol in Ret {
                        if existingCol.name == col.name {
                            existsInRet = true;
                            break;
                        }
                    }
                    if !existsInRet {
                        uNames.push(col.name);
                    }
                }

                // If the list of names from u0 is different from the list of names from u, throw "Union Branches Inconsistent"
                if u0Names.length() != uNames.length() {
                    return error("Union Branches Inconsistent: column count mismatch");
                }
                foreach int j in 0 ..< u0Names.length() {
                    if u0Names[j] != uNames[j] {
                        return error("Union Branches Inconsistent: column names or order mismatch");
                    }
                }
            }

            // For each Column col in u0, append col to Ret (only new ones)
            foreach var colName in u0Names {
                Ret.push({name: colName});
            }
        }
    }

    return Ret;
}

// Function to merge two maps
isolated function merge(json m1, json m2) returns json|error {
    json result = check m1.clone().mergeJson(m2);
    return result;
}

// Row product function - creates cartesian product of arrays of maps
isolated function rowProduct(json[][] parts) returns json[]|error {
    json[] result = [{}];

    foreach json[] partialRows in parts {
        json[] newResult = [];

        foreach json partialRow in partialRows {
            foreach json row in result {
                json merged = check merge(partialRow, row);
                newResult.push(merged);
            }
        }

        result = newResult;
    }

    return result;
}

// Helper function to recursively normalize select array items
isolated function normalizeArrayItems(json[] selects) returns json[]|error {
    json[] normalizedItems = [];
    foreach var s in selects {
        normalizedItems.push(check normalize(s));
    }
    return normalizedItems;
}

isolated function normalize(json def) returns json|error {
    map<anydata> normalizedDef = check def.cloneWithType();

    if (normalizedDef.hasKey("forEach") || normalizedDef.hasKey("forEachOrNull")) {
        // Initialize select array if it doesn't exist
        if (!normalizedDef.hasKey("select")) {
            normalizedDef["select"] = [];
        }

        if (normalizedDef.hasKey("forEach")) {
            normalizedDef["type"] = "forEach";
        }
        else {
            normalizedDef["type"] = "forEachOrNull";
        }

        // Move unionAll to select if it exists
        if (normalizedDef.hasKey("unionAll")) {
            json[] selectArray = check normalizedDef["select"].cloneWithType();
            json[] newSelect = [{"unionAll": normalizedDef["unionAll"]}.toJson()];
            newSelect.push(...selectArray);
            normalizedDef["select"] = newSelect;
            _ = normalizedDef.remove("unionAll");
        }

        // Move column to select if it exists
        if (normalizedDef.hasKey("column")) {
            json[] selectArray = check normalizedDef["select"].cloneWithType();
            json[] newSelect = [{"column": normalizedDef["column"]}.toJson()];
            newSelect.push(...selectArray);
            normalizedDef["select"] = newSelect;
            _ = normalizedDef.remove("column");
        }

        // Recursively normalize each item in select
        json[] selects = check normalizedDef["select"].cloneWithType();
        normalizedDef["select"] = check normalizeArrayItems(selects);
        return normalizedDef.toJson();
    } else if normalizedDef.hasKey("repeat") {
        normalizedDef["type"] = "repeat";
        // Initialize select array if it doesn't exist
        if (!normalizedDef.hasKey("select")) {
            normalizedDef["select"] = [];
        }

        // Move unionAll to select if it exists
        if (normalizedDef.hasKey("unionAll")) {
            json[] selectArray = check normalizedDef["select"].cloneWithType();
            json[] newSelect = [{"unionAll": normalizedDef["unionAll"]}.toJson()];
            newSelect.push(...selectArray);
            normalizedDef["select"] = newSelect;
            _ = normalizedDef.remove("unionAll");
        }

        // Move column to select if it exists
        if (normalizedDef.hasKey("column")) {
            json[] selectArray = check normalizedDef["select"].cloneWithType();
            json[] newSelect = [{"column": normalizedDef["column"]}.toJson()];
            newSelect.push(...selectArray);
            normalizedDef["select"] = newSelect;
            _ = normalizedDef.remove("column");
        }

        // Recursively normalize each item in select
        json[] selects = check normalizedDef["select"].cloneWithType();
        normalizedDef["select"] = check normalizeArrayItems(selects);
        return normalizedDef.toJson();
    } else if normalizedDef.hasKey("select") && normalizedDef.hasKey("column") && normalizedDef.hasKey("unionAll") {
        // Normalize to select type
        normalizedDef["type"] = "select";
        json[] selects = check normalizedDef["select"].cloneWithType();
        json[] newSelects = [];
        newSelects.push({"column": normalizedDef["column"]}.toJson());
        newSelects.push({"unionAll": normalizedDef["unionAll"]}.toJson());
        newSelects.push(...selects);
        normalizedDef["select"] = newSelects;
        _ = normalizedDef.remove("column");
        _ = normalizedDef.remove("unionAll");

        // Recursively normalize each item in select
        normalizedDef["select"] = check normalizeArrayItems(newSelects);
        return normalizedDef.toJson();
    } else if normalizedDef.hasKey("select") && normalizedDef.hasKey("unionAll") {
        normalizedDef["type"] = "select";
        json[] selects = check normalizedDef["select"].cloneWithType();
        json[] newSelects = [];
        newSelects.push({"unionAll": normalizedDef["unionAll"]}.toJson());
        newSelects.push(...selects);
        normalizedDef["select"] = newSelects;
        _ = normalizedDef.remove("unionAll");

        // Recursively normalize each item in select
        normalizedDef["select"] = check normalizeArrayItems(newSelects);
        return normalizedDef.toJson();
    } else if normalizedDef.hasKey("select") && normalizedDef.hasKey("column") {
        normalizedDef["type"] = "select";
        json[] selects = check normalizedDef["select"].cloneWithType();
        json[] newSelects = [];
        newSelects.push({"column": normalizedDef["column"]}.toJson());
        newSelects.push(...selects);
        normalizedDef["select"] = newSelects;
        _ = normalizedDef.remove("column");
        // Recursively normalize each item in select
        normalizedDef["select"] = check normalizeArrayItems(newSelects);
        return normalizedDef.toJson();
    } else if normalizedDef.hasKey("column") && normalizedDef.hasKey("unionAll") {
        normalizedDef["type"] = "select";
        json[] newSelects = [];
        newSelects.push({"column": normalizedDef["column"]}.toJson());
        newSelects.push({"unionAll": normalizedDef["unionAll"]}.toJson());
        normalizedDef["select"] = newSelects;
        _ = normalizedDef.remove("column");
        _ = normalizedDef.remove("unionAll");
        // Recursively normalize each item in select
        normalizedDef["select"] = check normalizeArrayItems(newSelects);
        return normalizedDef.toJson();
    } else if (normalizedDef.hasKey("select")) {
        normalizedDef["type"] = "select";
        json[] selects = check normalizedDef["select"].cloneWithType();
        normalizedDef["select"] = check normalizeArrayItems(selects);
        return normalizedDef.toJson();
    } else {
        if (normalizedDef.hasKey("unionAll")) {
            normalizedDef["type"] = "unionAll";
            json[] unionAlls = check normalizedDef["unionAll"].cloneWithType();
            normalizedDef["unionAll"] = check normalizeArrayItems(unionAlls);
        }
        else if (normalizedDef.hasKey("column")) {
            normalizedDef["type"] = "column";
        }
        return normalizedDef.toJson();
    }
}

isolated function columnOperation(json selectExpression, json node) returns json[]|error {
    map<anydata> result = {};
    map<anydata> expression = <map<anydata>>selectExpression;

    if (!expression.hasKey("column")) {
        return error("No column specified in select expression");
    }
    map<anydata>[] columns = check expression["column"].cloneWithType();
    foreach var c in columns {
        if (!c.hasKey("path") || !(c["path"] is string)) {
            return error("Path is not specified or is not a string in column expression");
        }

        // TODO: Replace path expressions with constants specified in the view definition
        json[] vs = check fhirpath:getValuesFromFhirPath(node, <string>c["path"]);
        string recordKey = c.hasKey("name") && (c["name"] is string) ? <string>c["name"] : <string>c["path"];

        if c.hasKey("collection") && c["collection"] is boolean && <boolean>c["collection"] {
            result[recordKey] = vs;
        } else if vs.length() === 1 {
            result[recordKey] = vs[0];
        } else if vs.length() === 0 {
            result[recordKey] = ();
        }
        else {
            return error("Collection flag is false for path: " + <string>c["path"]);
        }
    }
    return [result.toJson()];
}

isolated function selectOperation(json selectExpression, json node) returns json[]|error {
    map<anydata> expression = <map<anydata>>selectExpression;
    if (!expression.hasKey("select")) {
        return error("No select specified in select expression");
    }

    // Filter based on "where" clause conditions
    if (expression.hasKey("where")) {
        json[] whereConditions = check expression["where"].cloneWithType();
        foreach var w in whereConditions {
            map<anydata> whereCondition = check w.cloneWithType();
            if (!whereCondition.hasKey("path")) {
                return error("'where' condition must have a 'path' property");
            }
            string wherePath = <string>whereCondition["path"];
            json[] vals = check fhirpath:getValuesFromFhirPath(node, wherePath);

            // Get the first value or null if empty
            json val = vals.length() > 0 ? vals[0] : ();

            // Assert that the value is either null or boolean
            if (val !== () && val !is boolean) {
                return error("'where' expression path should return 'boolean'");
            }

            // If value is false or null, exclude this node
            if (val === () || val === false) {
                return [];
            }
        }
    }

    if (expression.hasKey("resource")) {
        if (expression["resource"] !== node.resourceType) {
            return [];
        }
    }
    json[][] evalResult = [];
    foreach var s in <json[]>expression["select"] {
        json[] partialResult = check doEval(s, node);
        evalResult.push(partialResult);
    }

    return rowProduct(evalResult);
}

isolated function forEachOperation(json selectExpression, json node) returns json[]|error {
    map<anydata> expression = <map<anydata>>selectExpression;

    // Assert forEach is required
    if (!expression.hasKey("forEach")) {
        return error("forEach required");
    }

    string forEachPath = <string>expression["forEach"];

    // Evaluate FHIRPath expression to get nodes
    json[] nodes = check fhirpath:getValuesFromFhirPath(node, forEachPath);

    json[] results = [];

    // For each node, apply the select operation
    foreach json nodeItem in nodes {
        if (expression.hasKey("select")) {
            json selectExpr = {"select": expression["select"]}.toJson();
            json[] selectResults = check selectOperation(selectExpr, nodeItem);
            results.push(...selectResults);
        }
    }

    return results;
}

isolated function forEachOrNullOperation(json selectExpression, json node) returns json[]|error {
    map<anydata> expression = <map<anydata>>selectExpression;

    // Assert forEach is required
    if (!expression.hasKey("forEachOrNull")) {
        return error("forEachOrNull required");
    }

    string forEachOrNullPath = <string>expression["forEachOrNull"];

    // Evaluate FHIRPath expression to get nodes
    json[] nodes = check fhirpath:getValuesFromFhirPath(node, forEachOrNullPath);
    if nodes.length() == 0 {
        nodes = [{}];
    }

    json[] results = [];

    // For each node, apply the select operation
    foreach json nodeItem in nodes {
        if (expression.hasKey("select")) {
            json selectExpr = {"select": expression["select"]}.toJson();
            json[] selectResults = check selectOperation(selectExpr, nodeItem);
            results.push(...selectResults);
        }
    }

    return results;
}

// Helper function to check if all results have the same columns
isolated function arraysUnique(json[] results) returns int {
    map<boolean> uniqueColumnSets = {};

    foreach var item in results {
        if item is map<anydata> {
            // Sort keys and create a string representation
            string columnSet = item.keys().sort().toString();
            uniqueColumnSets[columnSet] = true;
        }
    }

    return uniqueColumnSets.length();
}

isolated function unionAllOperation(json selectExpression, json node) returns json[]|error {
    map<anydata> expression = <map<anydata>>selectExpression;

    // Assert unionAll exists
    if (!expression.hasKey("unionAll")) {
        return error("unionAll is required");
    }

    // FlatMap: evaluate each unionAll element and flatten the results
    json[] result = [];
    foreach var d in <json[]>expression["unionAll"] {
        json[] partialResult = check doEval(d, node);
        result.push(...partialResult);
    }

    // TODO: ideally, this should be done during the validation
    // Validate that all results have the same columns
    int uniqueCount = arraysUnique(result);

    if uniqueCount > 1 {
        return error(string `Union columns mismatch: found ${uniqueCount} different column sets`);
    }

    return result;
}

// Helper function to recursively traverse FHIR nodes
isolated function traverse(json currentNode, string[] paths, json[] result, boolean isRoot) returns error? {
    // Don't add the root node to results, only its children
    if !isRoot {
        result.push(currentNode);
    }

    // Recursively traverse using each path expression
    foreach string path in paths {
        // TODO: Replace path expressions with constants specified in the view definition

        // Evaluate FHIRPath expression to get child nodes
        json[] childNodes = check fhirpath:getValuesFromFhirPath(currentNode, path);

        foreach json childNode in childNodes {
            // Only traverse if it's an object (map)
            if childNode !is json[] {
                check traverse(childNode, paths, result, false);
            }
        }
    }
}

// Recursively traverse a FHIR node using path expressions
isolated function recursiveTraverse(string[] paths, json node, map<anydata> def) returns json[]|error {
    json[] result = [];

    // Start traversal from root node
    check traverse(node, paths, result, true);

    return result;
}

isolated function repeatOperation(json selectExpression, json node) returns json[]|error {
    map<anydata> expression = <map<anydata>>selectExpression;

    // Assert repeat exists
    if !expression.hasKey("repeat") {
        return error("repeat is required");
    }
    // Repeat must be an array
    if expression["repeat"] !is anydata[] {
        return error("repeat must be an array");
    }

    // Use recursiveTraverse to get all nodes at all depths.
    string[] paths = check expression["repeat"].cloneWithType();
    var nodes = check recursiveTraverse(paths, node, expression);
    json[] results = [];

    // For each node, apply the select operation
    foreach json nodeItem in nodes {
        json selectExpr = {"select": expression["select"]}.toJson();
        json[] selectResults = check selectOperation(selectExpr, nodeItem);
        results.push(...selectResults);
    }
    return results;
}

isolated function doEval(json selectExpression, json node) returns json[]|error {

    match check selectExpression.'type {
        "column" =>
        {
            return columnOperation(selectExpression, node);
        }
        "select" =>
        {
            return selectOperation(selectExpression, node);
        }
        "forEach" =>
        {
            return forEachOperation(selectExpression, node);
        }
        "forEachOrNull" =>
        {
            return forEachOrNullOperation(selectExpression, node);
        }
        "unionAll" =>
        {
            return unionAllOperation(selectExpression, node);
        }
        "repeat" =>
        {
            return repeatOperation(selectExpression, node);
        }
        _ =>
        {
            return [];
        }
    }

}

public isolated function evaluate(json[] resources, json viewDefinition) returns json[]|error {
    // Validate view definition structure
    _ = check validateColumns(viewDefinition, []);

    json noramalDef = check normalize(viewDefinition.clone());

    json[] results = [];
    foreach json 'resource in resources {
        json[] evalResult = check doEval(noramalDef, 'resource);
        // Accumulate results
        foreach var result in evalResult {
            results.push(result);
        }
    }

    return results;
}


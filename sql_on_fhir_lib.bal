import ballerinax/health.fhir.r4utils.fhirpath;

// Column type for validation
type ColumnDefinition record {
    string name;
};

# Type definition for custom FHIRPath extension functions
# + nodes - Array of FHIR nodes to process
# + return - Array of extracted values
public type GetResourceKeyFunction isolated function (json[] nodes) returns json[]|error;

# Type definition for getReferenceKey extension function with options
# + nodes - Array of FHIR nodes to process
# + resourceType - Optional resource type to filter by
# + return - Array of extracted reference keys
public type GetReferenceKeyFunction isolated function (json[] nodes, string? resourceType) returns json[]|error;

# Configuration record for FHIRPath extension functions
# + getResourceKey - Custom implementation for getResourceKey function
# + getReferenceKey - Custom implementation for getReferenceKey function
public type FhirPathExtensions record {|
    GetResourceKeyFunction getResourceKey?;
    GetReferenceKeyFunction getReferenceKey?;
|};

# Default implementation of getResourceKey - extracts id from nodes
# + nodes - Array of FHIR nodes
# + return - Array of resource ids
public isolated function defaultGetResourceKey(json[] nodes) returns json[]|error {
    json[] result = [];
    foreach json node in nodes {
        if node is map<json> && node.hasKey("id") {
            result.push(node["id"]);
        }
    }
    return result;
}

# Default implementation of getReferenceKey - extracts reference key from reference string
# + nodes - Array of FHIR nodes containing reference field
# + resourceType - Optional resource type to filter by
# + return - Array of extracted reference keys
public isolated function defaultGetReferenceKey(json[] nodes, string? resourceType) returns json[]|error {
    json[] result = [];
    foreach json node in nodes {
        if node is map<json> && node.hasKey("reference") {
            string reference = <string>node["reference"];
            // Remove double slashes
            string cleanRef = re `//`.replaceAll(reference, "");
            // Split by /_history to get base reference
            string[] historyParts = re `/_history`.split(cleanRef);
            string basePart = historyParts[0];

            // Split by / to get type and key
            string[] parts = re `/`.split(basePart);
            if parts.length() >= 2 {
                string refType = parts[parts.length() - 2];
                string key = parts[parts.length() - 1];

                if resourceType is () {
                    // No filter, return all keys
                    result.push(key);
                } else if resourceType == refType {
                    // Filter matches, return key
                    result.push(key);
                }
                // Otherwise, filter doesn't match, skip this node
            }
        }
    }
    return result;
}

// Store for extension functions (module-level to be accessible across functions)
isolated FhirPathExtensions currentExtensions = {};

// Helper to check if path contains getResourceKey() call
isolated function containsGetResourceKey(string path) returns boolean {
    return path.includes(".getResourceKey()") || path.startsWith("getResourceKey()");
}

// Helper to check if path contains getReferenceKey() call
isolated function containsGetReferenceKey(string path) returns boolean {
    return path.includes(".getReferenceKey(") || path.startsWith("getReferenceKey(");
}

// Helper to extract base path before extension function call
isolated function extractBasePath(string path, string funcName) returns string {
    // Check for function at start (without dot)
    string funcNameWithoutDot = funcName.startsWith(".") ? funcName.substring(1) : funcName;
    if path.startsWith(funcNameWithoutDot) {
        return "";
    }
    int? idx = path.indexOf(funcName);
    if idx is int && idx > 0 {
        return path.substring(0, idx);
    }
    return "";
}

// Helper to extract parameter from getReferenceKey call
isolated function extractReferenceKeyParam(string path) returns string? {
    int? startIdx = path.indexOf(".getReferenceKey(");
    int parenStart;
    if startIdx is () {
        // Check if it starts with getReferenceKey( (no leading dot)
        if path.startsWith("getReferenceKey(") {
            parenStart = "getReferenceKey(".length();
        } else {
            return ();
        }
    } else {
        parenStart = <int>startIdx + ".getReferenceKey(".length();
    }
    int? endIdx = path.indexOf(")", parenStart);
    if endIdx is () {
        return ();
    }
    string paramStr = path.substring(parenStart, <int>endIdx).trim();
    if paramStr.length() == 0 {
        return ();
    }
    // Remove surrounding quotes if present
    if (paramStr.startsWith("'") && paramStr.endsWith("'")) ||
        (paramStr.startsWith("\"") && paramStr.endsWith("\"")) {
        return paramStr.substring(1, paramStr.length() - 1);
    }
    return paramStr;
}

# Evaluates a FHIRPath expression with support for custom extension functions
# + node - The FHIR resource node to evaluate against
# + path - The FHIRPath expression
# + extensions - Optional custom extension functions
# + return - Array of values from the FHIRPath evaluation
isolated function evaluateFhirPath(json node, string path, FhirPathExtensions? extensions = ()) returns json[]|error {
    // Check for getResourceKey() function call
    if containsGetResourceKey(path) {
        string basePath = extractBasePath(path, ".getResourceKey()");
        json[] nodes = basePath.length() > 0 ? check fhirpath:getValuesFromFhirPath(node, basePath) : [node];
        GetResourceKeyFunction getResourceKeyFn = extensions?.getResourceKey ?: defaultGetResourceKey;
        return getResourceKeyFn(nodes);
    }

    // Check for getReferenceKey() function call with optional parameter
    if containsGetReferenceKey(path) {
        string basePath = extractBasePath(path, ".getReferenceKey(");
        json[] nodes = basePath.length() > 0 ? check fhirpath:getValuesFromFhirPath(node, basePath) : [node];
        string? resourceTypeParam = extractReferenceKeyParam(path);
        GetReferenceKeyFunction getReferenceKeyFn = extensions?.getReferenceKey ?: defaultGetReferenceKey;
        return getReferenceKeyFn(nodes, resourceTypeParam);
    }

    // Standard FHIRPath evaluation
    return fhirpath:getValuesFromFhirPath(node, path);
}

// Validates column definitions for duplicates and unionAll branch consistency
isolated function validateColumnsTyped(ViewDefinitionSelect[] selects, ColumnDefinition[] C) returns ColumnDefinition[]|error {
    ColumnDefinition[] ret = C.clone();

    foreach ViewDefinitionSelect sel in selects {
        // Validate column definitions within this select node
        foreach ViewDefinitionSelectColumn col in (sel.column ?: []) {
            foreach ColumnDefinition existing in ret {
                if existing.name == col.name {
                    return error("Column Already Defined: " + col.name);
                }
            }
            ret.push({name: col.name});
        }

        // Recurse into nested selects
        if sel.'select != () {
            ColumnDefinition[] validated = check validateColumnsTyped(sel.'select ?: [], ret);
            foreach ColumnDefinition c in validated {
                boolean exists = false;
                foreach ColumnDefinition existingCol in ret {
                    if existingCol.name == c.name {
                        exists = true;
                        break;
                    }
                }
                if !exists {
                    ret.push(c);
                }
            }
        }

        // Validate unionAll branches for consistency
        if sel.unionAll != () {
            ViewDefinitionSelect[] unionBranches = sel.unionAll ?: [];
            if unionBranches.length() > 0 {
                ColumnDefinition[] u0 = check validateColumnsTyped([unionBranches[0]], ret);
                string[] u0Names = [];
                foreach ColumnDefinition col in u0 {
                    boolean existsInRet = false;
                    foreach ColumnDefinition existingCol in ret {
                        if existingCol.name == col.name {
                            existsInRet = true;
                            break;
                        }
                    }
                    if !existsInRet {
                        u0Names.push(col.name);
                    }
                }

                foreach int i in 1 ..< unionBranches.length() {
                    ColumnDefinition[] u = check validateColumnsTyped([unionBranches[i]], ret);
                    string[] uNames = [];
                    foreach ColumnDefinition col in u {
                        boolean existsInRet = false;
                        foreach ColumnDefinition existingCol in ret {
                            if existingCol.name == col.name {
                                existsInRet = true;
                                break;
                            }
                        }
                        if !existsInRet {
                            uNames.push(col.name);
                        }
                    }
                    if u0Names.length() != uNames.length() {
                        return error("Union Branches Inconsistent: column count mismatch");
                    }
                    foreach int j in 0 ..< u0Names.length() {
                        if u0Names[j] != uNames[j] {
                            return error("Union Branches Inconsistent: column names or order mismatch");
                        }
                    }
                }

                foreach string name in u0Names {
                    ret.push({name: name});
                }
            }
        }
    }

    return ret;
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

// Determines the evaluation type for a ViewDefinitionSelect node
isolated function getOperationType(ViewDefinitionSelect sel) returns string {
    if sel.forEach != () { return "forEach"; }
    if sel.forEachOrNull != () { return "forEachOrNull"; }
    if sel.repeat != () { return "repeat"; }
    if sel.unionAll != () && sel.'select == () && sel.column == () { return "unionAll"; }
    if sel.column != () && sel.'select == () && sel.unionAll == () { return "column"; }
    return "select";
}

isolated function doEvalTyped(ViewDefinitionSelect sel, json node, FhirPathExtensions? extensions = ()) returns json[]|error {
    match getOperationType(sel) {
        "column" => { return columnOperationTyped(sel, node, extensions); }
        "select" => { return selectOperationTyped(sel, node, extensions); }
        "forEach" => { return forEachOperationTyped(sel, node, extensions); }
        "forEachOrNull" => { return forEachOrNullOperationTyped(sel, node, extensions); }
        "unionAll" => { return unionAllOperationTyped(sel, node, extensions); }
        "repeat" => { return repeatOperationTyped(sel, node, extensions); }
        _ => { return []; }
    }
}

isolated function columnOperationTyped(ViewDefinitionSelect sel, json node, FhirPathExtensions? extensions = ()) returns json[]|error {
    map<anydata> result = {};
    foreach ViewDefinitionSelectColumn c in (sel.column ?: []) {
        json[] vs = check evaluateFhirPath(node, c.path, extensions);
        if c.collection ?: false {
            result[c.name] = vs;
        } else if vs.length() === 1 {
            result[c.name] = vs[0];
        } else if vs.length() === 0 {
            result[c.name] = ();
        } else {
            return error("Collection flag is false for path: " + c.path);
        }
    }
    return [result.toJson()];
}

// Evaluates column, unionAll, then select children of a node against the given FHIR node,
// combining results via rowProduct. Used by select, forEach, forEachOrNull, and repeat operations.
isolated function evalSelectChildren(ViewDefinitionSelect sel, json node, FhirPathExtensions? extensions = ()) returns json[]|error {
    json[][] parts = [];
    if sel.column != () {
        parts.push(check columnOperationTyped(sel, node, extensions));
    }
    if sel.unionAll != () {
        parts.push(check unionAllOperationTyped(sel, node, extensions));
    }
    foreach ViewDefinitionSelect childSel in (sel.'select ?: []) {
        parts.push(check doEvalTyped(childSel, node, extensions));
    }
    return rowProduct(parts);
}

isolated function selectOperationTyped(ViewDefinitionSelect sel, json node, FhirPathExtensions? extensions = ()) returns json[]|error {
    return evalSelectChildren(sel, node, extensions);
}

isolated function forEachOperationTyped(ViewDefinitionSelect sel, json node, FhirPathExtensions? extensions = ()) returns json[]|error {
    json[] nodes = check evaluateFhirPath(node, sel.forEach ?: "", extensions);
    json[] results = [];
    foreach json nodeItem in nodes {
        results.push(...check evalSelectChildren(sel, nodeItem, extensions));
    }
    return results;
}

isolated function forEachOrNullOperationTyped(ViewDefinitionSelect sel, json node, FhirPathExtensions? extensions = ()) returns json[]|error {
    json[] nodes = check evaluateFhirPath(node, sel.forEachOrNull ?: "", extensions);
    if nodes.length() == 0 {
        nodes = [{}];
    }
    json[] results = [];
    foreach json nodeItem in nodes {
        results.push(...check evalSelectChildren(sel, nodeItem, extensions));
    }
    return results;
}

// Helper function to check if all results have the same columns
isolated function arraysUnique(json[] results) returns int {
    map<boolean> uniqueColumnSets = {};
    foreach var item in results {
        if item is map<anydata> {
            string columnSet = item.keys().sort().toString();
            uniqueColumnSets[columnSet] = true;
        }
    }
    return uniqueColumnSets.length();
}

isolated function unionAllOperationTyped(ViewDefinitionSelect sel, json node, FhirPathExtensions? extensions = ()) returns json[]|error {
    json[] result = [];
    foreach ViewDefinitionSelect branch in (sel.unionAll ?: []) {
        result.push(...check doEvalTyped(branch, node, extensions));
    }
    int uniqueCount = arraysUnique(result);
    if uniqueCount > 1 {
        return error(string `Union columns mismatch: found ${uniqueCount} different column sets`);
    }
    return result;
}

// Helper function to recursively traverse FHIR nodes
isolated function traverse(json currentNode, string[] paths, json[] result, boolean isRoot, FhirPathExtensions? extensions = ()) returns error? {
    // Don't add the root node to results, only its children
    if !isRoot {
        result.push(currentNode);
    }

    // Recursively traverse using each path expression
    foreach string path in paths {
        json[] childNodes = check evaluateFhirPath(currentNode, path, extensions);
        foreach json childNode in childNodes {
            // Only traverse if it's not an array
            if childNode !is json[] {
                check traverse(childNode, paths, result, false, extensions);
            }
        }
    }
}

// Recursively traverse a FHIR node using path expressions
isolated function recursiveTraverse(string[] paths, json node, FhirPathExtensions? extensions = ()) returns json[]|error {
    json[] result = [];
    check traverse(node, paths, result, true, extensions);
    return result;
}

isolated function repeatOperationTyped(ViewDefinitionSelect sel, json node, FhirPathExtensions? extensions = ()) returns json[]|error {
    json[] nodes = check recursiveTraverse(sel.repeat ?: [], node, extensions);
    json[] results = [];
    foreach json nodeItem in nodes {
        results.push(...check evalSelectChildren(sel, nodeItem, extensions));
    }
    return results;
}

# Evaluates FHIR resources against a view definition
# + resources - Array of FHIR resources to evaluate
# + viewDefinition - The view definition
# + extensions - Optional custom FHIRPath extension functions (getResourceKey, getReferenceKey)
# + return - Array of result rows or error
public isolated function evaluate(json[] resources, ViewDefinition viewDefinition, FhirPathExtensions? extensions = ()) returns json[]|error {
    _ = check validateColumnsTyped(viewDefinition.'select, []);

    json[] results = [];
    foreach json 'resource in resources {
        // Filter by resource type
        if 'resource is map<json> {
            if 'resource["resourceType"] != viewDefinition.'resource {
                continue;
            }
        } else {
            continue;
        }

        // Apply top-level where filters
        boolean include = true;
        foreach ViewDefinitionWhere w in (viewDefinition.'where ?: []) {
            json[] vals = check evaluateFhirPath('resource, w.path, extensions);
            json val = vals.length() > 0 ? vals[0] : ();
            if val !== () && val !is boolean {
                return error("'where' expression path should return 'boolean'");
            }
            if val === () || val === false {
                include = false;
                break;
            }
        }
        if !include {
            continue;
        }

        // Evaluate each top-level select and combine via row product
        json[][] parts = [];
        foreach ViewDefinitionSelect sel in viewDefinition.'select {
            parts.push(check doEvalTyped(sel, 'resource, extensions));
        }
        results.push(...check rowProduct(parts));
    }

    return results;
}

package io.kestra.plugin.transform.arrow.jq;

import com.fasterxml.jackson.databind.JsonNode;
import net.thisptr.jackson.jq.BuiltinFunctionLoader;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Versions;

import java.util.ArrayList;
import java.util.List;

/**
 * Compiles one jq expression and evaluates it against Jackson trees.
 * <p>
 * Builtins are loaded once per class loader. Each evaluation gets a child scope so
 * variables set by the expression cannot leak into the next row.
 */
public final class JqEvaluator {
    private static final Scope ROOT_SCOPE = loadRootScope();

    private final JsonQuery query;

    public JqEvaluator(String expression) {
        try {
            this.query = JsonQuery.compile(expression, Versions.JQ_1_6);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid jq expression: " + e.getMessage(), e);
        }
    }

    public List<JsonNode> apply(JsonNode input) throws Exception {
        List<JsonNode> out = new ArrayList<>();
        query.apply(Scope.newChildScope(ROOT_SCOPE), input, out::add);
        return out;
    }

    private static Scope loadRootScope() {
        try {
            Scope scope = Scope.newEmptyScope();
            BuiltinFunctionLoader.getInstance().loadFunctions(Versions.JQ_1_6, scope);
            return scope;
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}

package edu.tcu.cs.hogwartsartifactsonline.hadoop;

import java.util.List;

public class HdfsOps {

    public long queryForLong(String sql, int timeoutSeconds) {
        List<List<Object>> rows = query(sql, timeoutSeconds);

        if (rows.isEmpty() || rows.get(0).isEmpty()) {
            throw new IllegalStateException("No result for query: " + sql);
        }

        Object value = rows.get(0).get(0);
        if (!(value instanceof Number)) {
            throw new IllegalStateException("Expected numeric result");
        }

        return ((Number) value).longValue();
    }
}

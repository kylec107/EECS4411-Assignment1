package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;
import java.util.Map;

/**
 * IndexedRowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 *
 * Also has a tree index on column `indexColumn`, which points
 * to all row indices with the given value.
 */
public class IndexedRowTable implements Table {

    int numCols;
    int numRows;
    private TreeMap<Integer, IntArrayList> index;
    private ByteBuffer rows;
    private int indexColumn;

    public IndexedRowTable(int indexColumn) {
        this.indexColumn = indexColumn;
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        // TODO: Implement this!
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
        this.index = new TreeMap<>();

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                int value = curRow.getInt(ByteFormat.FIELD_LEN * colId);
                this.rows.putInt(offset, value);

                if (colId == indexColumn) {
                    if (!index.containsKey(value)) {
                        index.put(value, new IntArrayList());
                    }
                    index.get(value).add(rowId);
                }
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        // TODO: Implement this!
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        return rows.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        // TODO: Implement this!
        int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        rows.putInt(offset,field);

        if (colId == indexColumn) {
            if (!index.containsKey(field)) {
                index.put(field, new IntArrayList());
            }
            index.get(field).add(rowId);
        }
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        // TODO: Implement this!
        int sum = 0;

        for (int rowId = 0; rowId < numRows; rowId++) {
            sum += getIntField(rowId, 0);
        }
        return sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        // TODO: Implement this!
        int sum = 0;

        if (indexColumn == 1) {
            Map<Integer, IntArrayList> temp = index.tailMap(threshold1, false);
            for (Map.Entry<Integer, IntArrayList> entry : temp.entrySet()) {
                for (int rowID : entry.getValue()) {
                    if (getIntField(rowID, 2) < threshold2) {
                        sum += getIntField(rowID, 0);
                    }
                }
            }
        }

        else if (indexColumn == 2) {
            Map<Integer, IntArrayList> temp = index.headMap(threshold2);
            for (Map.Entry<Integer, IntArrayList> entry : temp.entrySet()) {
                for (int rowId : entry.getValue()) {
                    if (getIntField(rowId, 1) > threshold1) {
                        sum += getIntField(rowId, 0);
                    }
                }
            }
        }

        else {
            for (int rowId = 0; rowId < numRows; rowId++) {
                if (getIntField(rowId, 1) > threshold1 && getIntField(rowId, 2) < threshold2) {
                    sum += getIntField(rowId, 0);
                }
            }
        }

        return sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        // TODO: Implement this!
        int sum = 0;

        if (indexColumn == 0) {
            Map<Integer, IntArrayList> temp = index.tailMap(threshold, false);

            for (Map.Entry<Integer, IntArrayList> entry : temp.entrySet()) {
                for (int rowId : entry.getValue()) {
                    for (int colId = 0; colId < numCols; colId++) {
                        sum += getIntField(rowId, colId);
                    }
                }
            }
        }

        else {
            for (int colId = 0; colId < numCols; colId++) {
                for (int rowId = 0; rowId < numRows; rowId++) {
                    if (getIntField(rowId, 0) > threshold) {
                        sum += getIntField(rowId, colId);
                    }
                }
            }
        }

        return sum;
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        // TODO: Implement this!
        int numUpdates = 0;

        if (indexColumn == 0) {
            Map<Integer, IntArrayList> temp = index.headMap(threshold);

            for (Map.Entry<Integer, IntArrayList> entry : temp.entrySet()) {
                for (int rowId : entry.getValue()) {
                    int temp1 = getIntField(rowId, 2);
                    int temp2 = getIntField(rowId, 3);
                    putIntField(rowId, 3, temp1 + temp2);
                    numUpdates++;
                }
            }
        }

        else {
                for (int rowId = 0; rowId < numRows; rowId++) {
                    if (getIntField(rowId, 0) < threshold) {
                        int temp1 = getIntField(rowId, 2);
                        int temp2 = getIntField(rowId, 3);
                        putIntField(rowId, 3, temp1 + temp2);
                        numUpdates++;
                    }
                }
            }

        return numUpdates;
    }
}

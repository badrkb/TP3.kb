import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import static java.lang.System.in;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Created by KBITAT on 03/11/2016.
 */
public class HbaseSocialNetwork {
    final static String REPL_TXT = "\nHBSN > ";
    final static String TABLE_NAME = "bkbitat";
    private static HTable table = null;
    private static BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    private static Configuration config = HBaseConfiguration.create();
    private static replState replStatus;

    private enum replState {
        WAITING, BUSY, EXIT
    }

    public static void main(String[] args) throws IOException {
        table = new HTable(config, TABLE_NAME);
        replStatus = replState.WAITING;

        while (replStatus == replState.WAITING || replStatus == replState.BUSY) {
            replStatus = replState.WAITING;
            repl();
            String enteredCommand = reader.readLine();
            switch (enteredCommand) {
                case "put":
                    replStatus = replState.BUSY;
                    putRow();
                    break;
                case "get":
                    replStatus = replState.BUSY;
                    getRow();
                    break;
                case "delete":
                    replStatus = replState.BUSY;
                    deleteRow();
                    break;
                case "help":
                    showHelp();
                    break;
                case "exit":
                    replStatus = replState.EXIT;
                    break;
                default:
                    System.out.println(new StringBuilder()
                            .append("\"").append(enteredCommand).append("\" ")
                            .append("is an invalid entry.\n"));
                    showHelp();
                    break;
            }

        }
    }

    private static void repl() {
        if (replStatus == replState.BUSY) {
            System.out.print("\t ");
        } else {
            System.out.print(REPL_TXT);
        }
    }

    private static void showHelp() {
        System.out.println("\thelp: This help");
        System.out.println("\tput: Put a new row (along with CF, qualifier and its value)");
        System.out.println("\tget: Get a row");
        System.out.println("\tdelete: Delete a row");
        System.out.println("\texit: Exit this REPL");
    }

    private static void getRow() throws IOException {
        System.out.print("\t[ROW ID] > ");
        String rowName = reader.readLine();

        System.out.println(new StringBuilder().append("Getting row: ").append(rowName));

        Get g = new Get(Bytes.toBytes(rowName));
        Result getResult = table.get(g);

        System.out.println(new StringBuilder().append(getResult).append("\n"));
    }

    private static void putRow() throws IOException {

        System.out.print("\t[ROW ID] > ");
        String rowName = reader.readLine();

        String columnFamily = "";
        List<String> authorizedCF = Arrays.asList("friends", "info");
        while (!authorizedCF.contains(columnFamily)) {
            System.out.print("\t[CF] (\"friends\" or \"info\") > ");
            columnFamily = reader.readLine();
        }

        String qualifier = "";
        if (columnFamily.equals("friends")) {
            List<String> authorizedQualifier = Arrays.asList("BFF", "others");
            while (!authorizedQualifier.contains(qualifier)) {
                System.out.print("\t[QUALIFIER] (\"BFF\" or \"others\") > ");
                qualifier = reader.readLine();
            }
        } else if (columnFamily.equals("info")) {
            System.out.print("\t[QUALIFIER] > ");
            qualifier = reader.readLine();
        }

        System.out.print("\t[VALUE] > ");
        String value = reader.readLine();

        String resultBFF = "";
        if (qualifier.equals("BFF")) {
            Get verifyBestFriend = new Get(Bytes.toBytes(value));
            resultBFF = table.get(verifyBestFriend).toString();
        }

        if (!resultBFF.equals("keyvalues=NONE")) {
            System.out.println(new StringBuilder().append("Putting row: ").append(rowName));

            Put p = new Put(Bytes.toBytes(rowName));

            p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(p);

            System.out.println(new StringBuilder().append("Row ").append(rowName).append(" added.\n"));
        } else {
            System.out.println("Is this an imaginary best friend? :(\n No row added.");
        }
    }

    private static void deleteRow() throws IOException {
        System.out.print("\t[ROW ID] > ");
        String rowName = reader.readLine();

        System.out.println(new StringBuilder().append("Deleting row: ").append(rowName));

        Delete d = new Delete(Bytes.toBytes(rowName));
        table.delete(d);

        System.out.println(new StringBuilder().append("Row ").append(rowName).append(" deleted.\n"));
    }

}

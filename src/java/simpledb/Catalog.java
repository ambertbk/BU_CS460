package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 *
 * @Threadsafe
 */
public class Catalog {

    // some code goes here
    public class Table {
        private DbFile file;  //contents of the table to add
        private String name;  //the name of the table
        private String pkey;  //the name of the primary key field

        //table class constructor
        public Table(DbFile i, String j, String k) {
            this.file = i;
            this.name = j;
            this.pkey = k;
        }
    }

    //used for catalog constructor
    private Map<Integer, Table> id_to_table;
    private Map<String, Table> name_to_table;
    private Map<String, Integer> name_to_id;
    private Map<Integer, String> id_to_name;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        id_to_table = new HashMap<Integer, Table>();
        name_to_id = new HashMap<String, Integer>();
        name_to_table = new HashMap<String, Table>();
        id_to_name = new HashMap<Integer, String>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        if (name == null) {
            throw new NullPointerException();
        }
        if (name_to_id.containsKey(name)) {
            //remove last table and then add new table
            id_to_table.remove(file.getId());
            name_to_table.remove(name);
            name_to_id.remove(name);
            id_to_name.remove(file.getId());
        }
        Table new_table = new Table(file, name, pkeyField);
        //add the new table to catalog
        id_to_table.put(file.getId(), new_table);
        name_to_table.put(name,new_table);
        name_to_id.put(name,file.getId());  //correspond name with field
        id_to_name.put(file.getId(), name);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) {
            throw new NoSuchElementException();
        }
        else if (!(name_to_id.containsKey(name))) {
            throw new NoSuchElementException();
        }
        int result = name_to_id.get(name);
        return result;
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        if (!(id_to_table.containsKey(tableid))) {
            throw new NoSuchElementException();
        } else {
            Table t = id_to_table.get(tableid);
            TupleDesc result = t.file.getTupleDesc();
            return result;
        }
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        if (!(id_to_table.containsKey(tableid))) {
            throw new NoSuchElementException();
        } else {
            DbFile result = id_to_table.get(tableid).file;
            if (result == null) {
                throw new NoSuchElementException();
            }
            return result;
        }
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        if (!(id_to_table.containsKey(tableid))) {
            throw new NoSuchElementException();
        } else {
            String result = id_to_table.get(tableid).pkey;
            if (result == null) {
                throw new NoSuchElementException();
            }
            return result;
        }
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        // get all keys from id_to_table
        Iterator<Integer> itr = id_to_table.keySet().iterator();
        return itr;
    }

    public String getTableName(int id) {
        // some code goes here
        if (!(id_to_name.containsKey(id))) {
            throw new NoSuchElementException();
        } else {
            String fname = id_to_name.get(id);
            return fname;
        }
    }

    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        id_to_table.replaceAll( (k,v)->v=null );
        name_to_table.replaceAll( (k,v)->v=null );
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}


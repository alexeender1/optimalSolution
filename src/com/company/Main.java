package com.company;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;


public class Main {
    
    
	/**
	 * format uset to create file name for bad records
	 */
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");

    public static void main(String[] args) throws ClassNotFoundException{
       
    	
    	Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    	Class.forName("org.sqlite.JDBC");
    	
		
        try (
                Reader reader = Files.newBufferedReader(Paths.get("csvFiles/Interview-task-data-osh.csv"));
                CSVReader csvReader = new CSVReader(reader);
        		Connection connection = DriverManager.getConnection("jdbc:sqlite::memory:");
        		FileWriter fileWriter = new FileWriter("csvFiles/bad-data-"+sdf.format(timestamp)+".csv");
        		FileOutputStream os = new FileOutputStream(new File("csvFiles/bad-data-"+sdf.format(timestamp)+".csv"));
                CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(os,Charset.forName("UTF8")));
        ) {
        	
        	createFilTable(connection);
            String[] nextRecord = new String[10];
           
            int allRecords = 0;
            int failedRecords = 0;
            int succesRecords = 0;
            csvReader.readNext();
            while ((nextRecord = csvReader.readNext()) != null) {
            	allRecords++;
            	nextRecord = Arrays.copyOf(nextRecord, 10);
                if(isBadRow(nextRecord)){
                    insertBadRowToFile(csvWriter,nextRecord);
                    failedRecords++;
                }else{
                    inserRowToDB(connection,nextRecord);
                    succesRecords++;
                }

            }
            
           
            //showDbContent(connection);
            logAction(String.valueOf(allRecords) + " records recieved !");
            logAction(String.valueOf(failedRecords) + " records failed !");
            logAction(String.valueOf(succesRecords) + " records inserted successfuly !");
           
        }catch(SQLException eSql) {
        	logAction("Insert file to DB error !!!");
        }catch(IOException  e) {
        	logAction("File reading error or in csvFiles directory exist directory like "+"'csvFiles/bad-data-"+sdf.format(timestamp)+".csv'");
        }
       
    }

    /**
     * insert an array of strings to DB
     * @param connection is connection to DB from memory
     * @param csvRow is an array of strings to inser in DB
     * @throws SQLException in case if some string from array is not normalised
     */
    static private void inserRowToDB(Connection connection,String[] csvRow) throws SQLException{

            Statement statement = connection.createStatement();
            csvRow =  remakeArrayQuotes(csvRow);
            String sql = "insert into file values('" + csvRow[0] + "','" + csvRow[1] + "','" + csvRow[2] + "','" + csvRow[3] + "','" + csvRow[4] + "','" + csvRow[5] + "','" + csvRow[6] + "','" + csvRow[7] + "','" + csvRow[8] + "','" + csvRow[9] + "')";
            statement.executeUpdate(sql);

    }

    /**
     * verify if array is ok by lenght and insert array string to csv file
     * @param csvWriter
     * @param csvRow
     */
    static private void insertBadRowToFile(CSVWriter csvWriter,String[] csvRow){

        if(csvRow.length<10){
            return;
        }
        
        csvWriter.writeNext(new String[]{csvRow[0],csvRow[1],csvRow[2],csvRow[3],csvRow[4],csvRow[5],csvRow[6],csvRow[7],csvRow[8],csvRow[9]});

    }

    /**
     * create sqllite memory table like csv file with 10 columns
     * @param connection to memory DB
     * @throws SQLException in case if memory error
     */
   static private void createFilTable(Connection connection) throws SQLException{

            Statement statement = connection.createStatement();

            statement.executeUpdate("create table file(A varchar(100) not null,\n" +
                    "B varchar(100) not null,\n" +
                    "C varchar(100) not null,\n" +
                    "D varchar(100) not null,\n" +
                    "E blob not null,\n" +
                    "F varchar(100) not null,\n" +
                    "G varchar(100) not null,\n" +
                    "H varchar(100) not null,\n" +
                    "I varchar(100) not null,\n" +
                    "J varchar(100) not null)");
        
    }

   /**
    * verify if array contains null or empty cell
    * @param row is an string array 
    * @return true if exist null or empty cell
    */
    static private boolean isBadRow(String[] row){
    	if(row == null || row.length == 0) {
    		return false;
    	}
        List<String> rowCells = Arrays.asList(row);
        if(rowCells.contains("") || rowCells.contains(null)){
            return true;
        }
        return false;
    }

    /**
     * add double quotes in front and end of cell contant if
     * content contain one quote and make this quote possible to insert
     * in db 
     * @param badArray array with one or more cell that contains quote
     * @return normalized array for insert in DB
     */
    static private String[] remakeArrayQuotes(String[] badArray){
        if (badArray == null){
            return new String[10];
        }
        String[] clearArray = new String[badArray.length];

        for(int i = 0; i<badArray.length;i++) {
            if (badArray[i].contains("\'")) {
                badArray[i] = "\"" +badArray[i].replace("\'", "\'\'")+"\"";
                
            }
            clearArray[i] = badArray[i];
        }

                return clearArray;
    }
    
    static private void showDbContent(Connection connection) throws SQLException {
    	Statement statement = connection.createStatement();
        statement.setQueryTimeout(30);
        
        ResultSet rs = statement.executeQuery("Select * from file");
        int dbRows = 0;
        while (rs.next()) 
        { 
        	dbRows++;
            // read the result set 
            System.out.print(rs.getString(1)+" "); 
            System.out.print(rs.getString(2)+" ");
            System.out.print(rs.getString(3)+" ");
            System.out.print(rs.getString(4)+" "); 
            System.out.print(rs.getString(5)+" ");
            System.out.print(rs.getString(6)+" ");
            System.out.print(rs.getString(7)+" "); 
            System.out.print(rs.getString(8)+" ");
            System.out.print(rs.getString(9)+" ");
            System.out.print(rs.getString(10)+" ");
            System.out.println();
        } 
        System.out.println("rows in db = "+dbRows);
    }
    
    /**
     * insert an message in logActivity.log file
     * @param message is an string to insert in file
     */
    static private void logAction(String message) {
    	
    	File file = new File("csvFiles/logActivity.log");
        
        try(	FileWriter fw = new FileWriter(file, true);
        		BufferedWriter bw = new BufferedWriter(fw)) {
            
            bw.write(message);
            bw.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        } 
    }
}

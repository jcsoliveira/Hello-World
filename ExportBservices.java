package pt.inpi.Bservices;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileOutputStream;

//import java.lang.StringBuilder;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import pt.inpi.Bservices.UserNotDefinedException;



public class ExportBservices {
	
	final static Logger logger = Logger.getLogger(ExportBservices.class);
	
	public static final String MY_DATE_FORMAT = "yyyyMMdd";
	public static final String SMBFileSeparator = "/";
	private static String BASEDIR  ; 	
	private static String ent_externas_path_in ;
	private static String ent_externas_path_out;
	private static String mule_fileRepository_csv_path;
	private static String mule_fileRepository_pdf_path;
	private static String csv_temporary_local_dir;
	private static String connectionString_sgpi_ext;
	private static String connectionString_btob;
	private static String user_sgpi_ext;
	private static String pw_sgpi_ext;
	private static String user_sgpi_btob;
	private static String pw_sgpi_btob;
	private static String domain_NetworkFolder;
	private static String user_NetworkFolder;
	private static String pw_NetworkFolder;
	private static String path_to_log4j_properties;

	
	
	public ExportBservices() {

	}
	
	private static void cleanMetadata (String sDate){
		
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection conn = DriverManager.getConnection
			(connectionString_btob,user_sgpi_btob,pw_sgpi_btob);

			String sql = "DELETE FROM DOCUMENT_INPI_EM_REDE WHERE (DO_IR_FOLDER_DATE = ?)";

			PreparedStatement ps = null;		
			ResultSet rset;

			ps = conn.prepareStatement(sql);
			ps.setString(1, sDate);
			rset = ps.executeQuery();
			
			conn.commit();
			conn.close();

		}
		catch (SQLException e) {
			logger.error("Bservices ERRO na limpeza da tabela DOCUMENT_INPI_EM_REDE. Efectue a limpeza manual dos meta-dados com data: " + sDate + e.getMessage());
			logger.error("Bservices ERRO: " + e.getMessage());
			e.printStackTrace();

		}
		catch (ClassNotFoundException e) {
			logger.error("Bservices ERRO na limpeza da tabela DOCUMENT_INPI_EM_REDE. Efectue a limpeza manual dos meta-dados com data: " + sDate + e.getMessage());
			logger.error("Bservices ERRO: " + e.getMessage());
			e.printStackTrace();
		} catch (Exception e){
			logger.error("Bservices ERRO na limpeza da tabela DOCUMENT_INPI_EM_REDE. Efectue a limpeza manual dos meta-dados com data: " + sDate + e.getMessage());
			logger.error("Bservices ERRO: " + e.getMessage());
			e.printStackTrace();		
		}
		
	}
	
	private static void cleanupFilesAndMetadataWhenCrash (String sDate){

		cleanMetadata (sDate);

		try {
			File csvDir  = new File(csv_temporary_local_dir);   
			String[] csvFiles;    
			if (csvDir.isDirectory()) {
				csvFiles = csvDir.list();
				for (int i = 0; i < csvFiles.length; i++) {
					File deleteFile = new File(csvDir, csvFiles[i]); 
					deleteFile.delete();
				}
			}else {
				logger.error("Não foi possível limpar os ficheiros nas directorias após ter ocorrido um erro critico. POr favor verifique as directorias e proceda a uma limpeza manual dos ficheiros");
			}
			NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(domain_NetworkFolder, user_NetworkFolder,pw_NetworkFolder);
			SmbFile pdfDir  = new SmbFile(mule_fileRepository_pdf_path+SMBFileSeparator,auth);   
			String[] pdfFiles;    
			if (pdfDir.isDirectory()) {
				pdfFiles = pdfDir.list();
				for (int i = 0; i < pdfFiles.length; i++) {
					SmbFile deleteFile = new SmbFile(pdfDir.getPath()+SMBFileSeparator+ pdfFiles[i]); 
					deleteFile.delete();
				}
			}else {
				logger.error("Não foi possível apagar os ficheiros nas directorias após ter ocorrido um erro critico. POr favor verifique as directorias e proceda a uma limpeza manual dos ficheiros");
			}
		}catch (IOException e) {
			logger.error("Bservices ERRO:  Não foi possível apagar os ficheiros CSV para o Mule no processo de limpeza. O processo terá que ser conduzido manualmente " + e.getMessage());	
			e.printStackTrace();
		}catch (Exception e){
			logger.error("Bservices ERRO:  Não foi possível apagar os ficheiros CSV para o Mule no processo de limpeza. O processo terá que ser conduzido manualmente " + e.getMessage());	
			e.printStackTrace();		
		}

	}


	private static void copyCsvFromTempToMule(File csvTempDir, SmbFile csvFinalDir) 
				throws IOException, SmbException, MalformedURLException, UnknownHostException {
		
		NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(domain_NetworkFolder, user_NetworkFolder,pw_NetworkFolder);

		if (csvTempDir.isDirectory())
		{

			String files[] = csvTempDir.list();

			for (String file : files)
			{
				
				File srcFile = new File(csvTempDir, file);
				SmbFile destFile = new SmbFile(csvFinalDir.getPath() + SMBFileSeparator + file,auth);

				copyCsvFromTempToMule(srcFile, destFile);
			}
		}
		else
		{
			try {
//				FileChannel source = null;
				SmbFileOutputStream destination = null;
				SmbFile destinationFile = new SmbFile(csvFinalDir.getPath(),auth);

//				source = new FileInputStream(csvTempDir).getChannel();
				
				
				destination = new SmbFileOutputStream(destinationFile);
				
				FileInputStream inCsv = new FileInputStream(csvTempDir);

				if (destination != null && inCsv != null) {
					byte[] b = new byte[8192];
					int n= 0;
					while(( n = inCsv.read( b )) > 0 ) {
						destination.write( b, 0, n );
					}
				}
				if (inCsv != null) {
					inCsv.close();
				}
//				if (source != null) {
//					source.close();
//				}
				
				csvTempDir.delete();

				
				if (destination != null) {
					destination.close();
				}
				csvTempDir.delete();

			} catch (IOException e) {
				logger.error("Bservices ERRO:  Não foi possível transferir os ficheiros CSV para o Mule. O processo terá que ser conduzido manualmente" + e.getMessage());	
				e.printStackTrace();
				throw e;
			}		
		}
	}
	
	private static void readGlobalParameters(Properties props) throws IOException {
		// TODO Auto-generated method stub

		try{
			String propFile = ClassLoader.getSystemClassLoader().getResource(".").getPath()+"Bservices.properties";
			//propFile= propFile.substring(1);
			
			System.out.println("propFile name: " + propFile);
			logger.info("propFile name: " + propFile);
			
			File f = new File(propFile);
			InputStream propertyFile = null;
			if (!f.exists())
				props.load(ExportBservices.class.getResourceAsStream("Bservices.properties"));
			else
				props.load(new FileInputStream(f));
			
			ent_externas_path_in = props.getProperty("ent_externas_path_in");
			BASEDIR = props.getProperty("BASEDIR"); 
			ent_externas_path_out = props.getProperty("ent_externas_path_out"); 
			mule_fileRepository_csv_path = props.getProperty("mule_fileRepository_csv_path");
			mule_fileRepository_pdf_path = props.getProperty("mule_fileRepository_pdf_path");
			csv_temporary_local_dir = props.getProperty("csv_temporary_local_dir");
			connectionString_sgpi_ext = props.getProperty("connectionString_sgpi_ext");
			connectionString_btob = props.getProperty("connectionString_btob");
			user_sgpi_ext = props.getProperty("user_sgpi_ext");
			pw_sgpi_ext = props.getProperty("pw_sgpi_ext");
			user_sgpi_btob = props.getProperty("user_sgpi_btob");
			pw_sgpi_btob = props.getProperty("pw_sgpi_btob");
			domain_NetworkFolder = props.getProperty("domain_NetworkFolder");
			user_NetworkFolder = props.getProperty("user_NetworkFolder");
			pw_NetworkFolder = props.getProperty("pw_NetworkFolder");
			path_to_log4j_properties=props.getProperty("path_to_log4j_properties");
			
		}catch (IOException e) {
			e.printStackTrace();
			throw e;
		}	
	}


	
	public static void main(String[] args) {

		class ProcessBServicesAnswers {
			
			class FileNameExtension{
				
				private String filename;
				private long extension;
				
				public FileNameExtension(String filename, long extension){
					this.extension = extension;
					this.filename = filename;
				}
				public String getFilename(){
					return this.filename;
				}
				public long getExtension(){
					return this.extension;
				}
			}
			


			
			List<FileNameExtension> pdfFiles = new ArrayList<FileNameExtension>();
			

			
			private void  insertpdfFileExtension (String filename, long fileextension){

				FileNameExtension thisFile = new FileNameExtension(filename,fileextension);
				try{
					pdfFiles.add (thisFile);
					Thread.sleep(100);				
				} catch (InterruptedException e){
					System.out.println("Exception thrown: " + e.getMessage());	
					logger.error("Bservices ERRO:" + e.getMessage());
				}

			}
			private long getPdfFileExtension(String filename){
				long extension =0;
				for (FileNameExtension fileNameExtension : pdfFiles) {
					if (fileNameExtension.getFilename().equals(filename)){
						extension = fileNameExtension.getExtension();
						pdfFiles.remove(fileNameExtension);
						break ;
					}
				}
				return extension;
			}
			private void create_BS_directories() throws IOException  {
				NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(domain_NetworkFolder, user_NetworkFolder,pw_NetworkFolder);
				
				boolean success = false;
				SmbFile smbDir ;
				File dir  = new File(ent_externas_path_in);
				if (!dir.exists()) {
					success = (new File(ent_externas_path_in)).mkdirs();
				}
							
				dir  = new File(ent_externas_path_out);
				if (!dir.exists()) {
					success = (new File(ent_externas_path_out)).mkdirs();
				}
				
				dir  = new File(csv_temporary_local_dir);
				if (!dir.exists()) {
					success = (new File(csv_temporary_local_dir)).mkdirs();
				}
				smbDir  = new SmbFile(mule_fileRepository_csv_path,auth);
				if (!smbDir.exists()) {
					(new SmbFile(mule_fileRepository_csv_path,auth)).mkdirs();
					success = true;
				}
				smbDir  = new SmbFile(mule_fileRepository_pdf_path,auth);
				if (!smbDir.exists()) {
					(new SmbFile(mule_fileRepository_pdf_path,auth)).mkdirs();
					success = true;
				}
				//*****************************
				Calendar cal = Calendar.getInstance();
				SimpleDateFormat sdf = new SimpleDateFormat(MY_DATE_FORMAT);
				String sCurrDate = sdf.format(cal.getTime());
								
				CreateInpiRedeServiceDir("BS_ADMINISTRATIVEOFFENCES",sCurrDate);
				CreateInpiRedeServiceDir("BS_JURIDICALSTATUS",sCurrDate);
				CreateInpiRedeServiceDir("BS_REQUESTEXPERTS",sCurrDate);
				CreateInpiRedeServiceDir("BS_JURIDICALADVICES",sCurrDate);
				CreateInpiRedeServiceDir("BS_ANNOTATIONS",sCurrDate);
				CreateInpiRedeServiceDir("BS_APPEALINPIDECISIONS",sCurrDate);
				CreateInpiRedeServiceDir("BS_COURTDECISIONS",sCurrDate);
			
			}			
			
			private void CreateInpiRedeServiceDir(String path, String sDate)
			{
				boolean success = false;
				File dir  = new File(ent_externas_path_in + SMBFileSeparator + "BS" + 
						SMBFileSeparator + path);
				if (!dir.exists()) {
					success = (new File(ent_externas_path_in + SMBFileSeparator + "BS" + 
							SMBFileSeparator + path)).mkdirs();
				}
				
				dir  = new File(ent_externas_path_in + SMBFileSeparator + "BS" + 
						SMBFileSeparator + path + SMBFileSeparator + sDate);
				if (!dir.exists()) {
					success = (new File(ent_externas_path_in + SMBFileSeparator + "BS" + 
							SMBFileSeparator + path + SMBFileSeparator + sDate)).mkdirs();					
				}	
			}
			
			private File [] getPdfFilesFromDir(File dir){

				File [] files = dir.listFiles(new FilenameFilter() {
				    @Override
				    public boolean accept(File dir, String name) {
				        return name.endsWith(".pdf");
				    }
				});

				for (File pdffile : files) {
					System.out.println("pdf file found = " + pdffile.getName());
				    System.out.println(pdffile);
				}
				return files;
			}

			private void do_bs_oficios_entidades_externas(String srcPath, String DestPath, String sDate) 
			throws UserNotDefinedException, ProcessNumberEmptyException, IOException, SQLException, ClassNotFoundException, FileNotFoundException, Exception {

				File root = new File( srcPath);
				File[] list = root.listFiles();

				try
				{
					if (list == null) return;

					for ( File f : list ) {
						if ( f.isDirectory() ) {
							do_bs_oficios_entidades_externas( f.getAbsolutePath(), DestPath, sDate);
							System.out.println( "Dir:" + f.getAbsoluteFile() );
							logger.info("Bservices: Dir: " + f.getAbsoluteFile());
						}
						else {
							System.out.println( "File:" + f.getAbsoluteFile() );
							logger.info("Bservices: File: " + f.getAbsoluteFile());
							if (f.getName().endsWith(".xlsx") || f.getName().endsWith(".XLSX") || f.getName().endsWith(".xls") || f.getName().endsWith(".XLS")){
								//Convert excel to csv and put csv available to Mule
								processExcelFile(f,sDate, DestPath);
							}
						}
					}

				}catch(ClassNotFoundException e){
					System.out.println("Exception:  " + e.getMessage());
					logger.error("Bservices: ERRO: " + e.getMessage());
					throw e;
				}catch(SQLException e){
					System.out.println("Exception:  " + e.getMessage());
					logger.error("Bservices: ERRO: " + e.getMessage());
					throw e;
				} catch (Exception e){
					System.out.println("Exception:  " + e.getMessage());
					logger.error("Bservices: ERRO: " + e.getMessage());	
					throw e;
				}

			}

			private  String getEntity(String user) throws SQLException, ClassNotFoundException{

				String entity ="";
				try{
					Class.forName("oracle.jdbc.driver.OracleDriver");
					Connection conn = DriverManager.getConnection
					//("jdbc:oracle:thin:@localhost:1521:PORTINES", "sgpi_ext", "Ssl#2017%");
					(connectionString_sgpi_ext, user_sgpi_ext,pw_sgpi_ext);

					String sqlgetEntity = "SELECT WEUT_GRUP FROM sgpi_ext.web_utilizador WHERE  WEUT_CODI=?";

					PreparedStatement ps = null;		
					ResultSet rset;

					ps = conn.prepareStatement(sqlgetEntity);
					ps.setString(1, user);
					rset = ps.executeQuery();
					if (rset.next()){
						entity = rset.getString(1);
					}
					conn.close();
				}
				catch (SQLException e) {
					logger.error("Bservices ERRO: " + e.getMessage());
					e.printStackTrace();
					throw e;
				}
				catch (ClassNotFoundException e) {
					logger.error("Bservices ERRO: " + e.getMessage());
					e.printStackTrace();
					throw e;
				}

				return entity.toUpperCase();

			}
			
			//pre-condition:
			// filenames defined by INPI technicians have to be with the following layout:
			//<Entidade>_<Data>_NrProcessoEntidade
			private void copyFiletoEntity(String finalPath, File file, String DestPath, String entity, String processNumber, String doc_path, String sDate)
							throws IOException, SQLException, ClassNotFoundException {

				FileChannel source = null;
				SmbFileOutputStream destinationEntity = null;
				SmbFileOutputStream destinationMule = null;

				long extension = getPdfFileExtension(file.getName());

				try{
					NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(domain_NetworkFolder, user_NetworkFolder,pw_NetworkFolder);
					SmbFile muleFile = new SmbFile(mule_fileRepository_pdf_path+SMBFileSeparator+extension+"#"+file.getName(),auth);
					SmbFile destFile = new SmbFile(finalPath+SMBFileSeparator+file.getName(), auth);
					source = new FileInputStream(file).getChannel();
					destinationEntity = new SmbFileOutputStream(destFile);
					destinationMule = new SmbFileOutputStream(muleFile);

					
					FileInputStream inMule = new FileInputStream(file);
					if (destinationMule != null && source != null) {
						byte[] b = new byte[8192];
						int n= 0;
						while(( n = inMule.read( b )) > 0 ) {
							destinationMule.write( b, 0, n );
						}
					}
					if (inMule != null) {
						inMule.close();
					}
					
					if (source != null) {
						source.close();
					}
					
												
					FileInputStream in = new FileInputStream(file);
					if (destinationEntity != null && source != null) {
						byte[] b = new byte[8192];
						int n= 0;
						while(( n = in.read( b )) > 0 ) {
						      destinationEntity.write( b, 0, n );
						}
					}
					if (in != null) {
						in.close();
					}
					if (destinationMule != null) {
						destinationMule.close();
					}
					if (destinationEntity != null) {
						destinationEntity.close();
					} 
					WriteDocToDataBase(file, entity, processNumber, doc_path, sDate);
				}
				catch (IOException e) {
					logger.error("Bservices ERRO: " + e.getMessage());	
					e.printStackTrace();
					throw e;
				}
				catch (SQLException e) {
					logger.error("Bservices ERRO: " + e.getMessage());
					e.printStackTrace();
					throw e;
				}
				catch (ClassNotFoundException e) {
					logger.error("Bservices ERRO: " + e.getMessage());
					e.printStackTrace();
					throw e;
				}

			}
			
			private void WriteDocToDataBase(File file, String entity, String processNumber, String doc_path, String sDate)throws SQLException, ClassNotFoundException {

				java.util.Date today = new java.util.Date();
				try {

					Class.forName("oracle.jdbc.driver.OracleDriver");

					// @machineName:port:SID,   userid,  password
					Connection conn = DriverManager.getConnection
					(connectionString_btob,user_sgpi_btob,pw_sgpi_btob);

					String sql = "UPDATE DOCUMENT_INPI_EM_REDE " +
					"SET CONFIDENTIAL =?, DOC_NAME=?, DOC_PATH=?, DOC_SIZE=?,entity=?,import_date=?,num_hits=? " +
					" WHERE DOC_NAME=? AND DO_IR_PROCNUMBER= ? AND DO_IR_FOLDER_DATE=?";
					PreparedStatement ps = null;		
					ResultSet rset;

					ps = conn.prepareStatement(sql);

					ps.setString(1, "N");

					//ps.setLong(2, getRandomID());
					ps.setString(2, file.getName());
					ps.setString(3, doc_path);

					ps.setLong(4,file.length());
					ps.setString(5, entity);
					ps.setTimestamp(6, new java.sql.Timestamp(today.getTime()));
					ps.setLong(7, 0);
					ps.setString(8, file.getName());
					ps.setString(9, processNumber);
					ps.setString(10,sDate);
					rset = ps.executeQuery();
					conn.commit();
					conn.close();


					System.out.println("fileName: " + file.getName());
					logger.info("Bservices, fileName: " + file.getName());
					System.out.println("Path: " + doc_path);
					logger.info("Bservices, Path: " + doc_path);
					System.out.println("file length: " + file.length());
					logger.info("Bservices, file length: " + file.length());
					System.out.println("entity: " + entity);
					logger.info("Bservices, entity: " + entity);
					System.out.println("Process Number: " + processNumber);
					logger.info("Bservices, Process Number: " + processNumber);
					System.out.println("time of insert: " + today.getTime());
					logger.info("Bservices, time of insert: " + today.getTime());
				} catch (SQLException e) {
					logger.error("Bservices ERRO: " + e.getMessage());
					e.printStackTrace();
					throw e;
				}
				catch (ClassNotFoundException e) {
					logger.error("Bservices ERRO: " + e.getMessage());
					e.printStackTrace();
					throw e;
				}
			}
			
			/*Campo do username do ficheiro excel: verificar se existe fazendo um select na base de dados
			 * fazendo um select na base de dados se vier vazio então abortar e apagar os dados criados na base de dados
			 * No fim Fazer o raise duma excepcao para ser catched no fim deste procedimento para fazer log4j a indicar que o username inserido nao existe na base de dados
			 * e que portanto este ficheiro excel tem entradas invalidas fazer o raise de uma excepcao e o procedimento deve fazer um throw para o procedimento acima
			 * para o processamento deste ficheiro excel.
			 */
			private void checkUsername(String username)throws UserNotDefinedException, SQLException, ClassNotFoundException {

				try {
					Class.forName("oracle.jdbc.driver.OracleDriver");
					Connection conn = DriverManager.getConnection
					//("jdbc:oracle:thin:@localhost:1521:PORTINES", "sgpi_ext", "Ssl#2017%");
					(connectionString_sgpi_ext, user_sgpi_ext,pw_sgpi_ext);

					String sqlUsername = "SELECT WEUT_GRUP FROM sgpi_ext.web_utilizador WHERE  WEUT_CODI=?";

					PreparedStatement ps = null;		
					ResultSet rset;

					ps = conn.prepareStatement(sqlUsername);
					ps.setString(1, username);
					rset = ps.executeQuery();

					if (!rset.next()){
						logger.error("Bservices ERROR. Process ABORTED. User not defined: " + username);
						throw new UserNotDefinedException("Requerente não encontrado : " + username);
					}
					conn.close();
				}catch (SQLException e){
					logger.error("Bservices ERRO: " + e.getMessage());
					System.out.println("Exception:  " + e.getMessage());
					throw e ;
				} catch (ClassNotFoundException e){
					logger.error("Bservices ERRO: " + e.getMessage());
					System.out.println("Exception:  " + e.getMessage());
					throw e ;
				}
			}			
			
			private String ReplaceInvalidChars(String originalStr)
			{
				String invalidCharRemoved = originalStr;
				if ((originalStr != null) && (originalStr.length() > 0))
					invalidCharRemoved = originalStr.replaceAll("[\\\\/:*?\"<>|]", "_");
				return invalidCharRemoved;
			}
			private String insertMetadataIntoBD(String requerente, String fillingNumber, String authorityProcessIdentification,
					String applicationTypeEnumType, String applicationNumber, String docNr, String nrOfDocuments, String filename,
					String comment,String documentType,
					String entity,String doc_path, String yearMonth,String sDate) throws SQLException, ClassNotFoundException {
					
				String result= "";
				try{
					Class.forName("oracle.jdbc.driver.OracleDriver");

					// @machineName:port:SID,   userid,  password
					Connection conn = DriverManager.getConnection (connectionString_btob,user_sgpi_btob,pw_sgpi_btob);
					ResultSet rset;					
					
					String sqlVerifyDataPresent = "select DOC_NAME, DO_IR_CODI, DO_IR_PROCNUMBER from document_inpi_em_rede "
											+ " where DOC_NAME=? AND DO_IR_PROCNUMBER=? AND DO_IR_FOLDER_DATE=?";
					PreparedStatement ps = conn.prepareStatement(sqlVerifyDataPresent);
					ps.setString(1, filename);
					ps.setString(2, authorityProcessIdentification);
					ps.setString(3, sDate);
					
					rset = ps.executeQuery();

		    		if (rset.next()){
		    			result += "NOK";
						
		    			System.out.println("Documento já registado na BD: " + filename + " com o processo nr: " + authorityProcessIdentification+
    					". O registo não foi actualizado");
		    			logger.info("Bservices: Documento já registado na BD: " + filename + " com o processo nr: " + authorityProcessIdentification+
    					". O registo não foi actualizado");
    			

		    		} else {
		    			String sqlInsert = "INSERT INTO DOCUMENT_INPI_EM_REDE (DOC_NAME, DO_IR_CODI, DO_IR_FNUMBER,DO_IR_PROCNUMBER,DO_IR_APPTYPE,DO_IR_APPNUMBER," +
		    			"DO_IR_DOCSEQ,DOC_IR_LASTSEQ,DO_IR_COMMENT,DO_IR_DOC_TYPE,DO_IR_FOLDER_DATE) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
						ps = conn.prepareStatement(sqlInsert);
						ps.setString(1, filename);
						ps.setString(2, requerente);
			    		ps.setString(3, fillingNumber);
			    		ps.setString(4, authorityProcessIdentification);
			    		ps.setString(5, applicationTypeEnumType);
			    		ps.setString(6, applicationNumber);
			    		ps.setString(7, docNr);
			    		ps.setString(8, nrOfDocuments);
			    		ps.setString(9, comment);
			    		ps.setString(10, documentType);
			    		ps.setString(11,sDate);
			    		rset = ps.executeQuery();
			    		conn.commit();
			    		result += "OK";
		    		}
		    		
					conn.close();
		    		
		    		System.out.println("Filename: " + filename);
		    		logger.info("Bservices, fileName: " + filename);
		    		System.out.println("requerente: " + requerente);
		    		logger.info("Bservices, requerente: " + requerente);
		    		System.out.println("filling number: " + fillingNumber);
		    		logger.info("Bservices, filling number: " + fillingNumber);
		    		System.out.println("authority ProcessNr: " + authorityProcessIdentification);
		    		logger.info("Bservices, authority ProcessNr: " + authorityProcessIdentification);
		    		System.out.println("application type: " + applicationTypeEnumType);
		    		logger.info("Bservices, application type: " + applicationTypeEnumType);
		    		System.out.println("application number: " + applicationNumber);
		    		logger.info("Bservices, application number: " + applicationNumber);
		    		System.out.println("document nr: " + docNr);
		    		logger.info("Bservices, document nr: " + docNr);
		    		System.out.println("Last document nr: " + nrOfDocuments);
		    		logger.info("Bservices, Last document nr: " + nrOfDocuments);
		    		System.out.println("Comment: " + comment);
		    		logger.info("Bservices, Comment: " + comment);
		    		System.out.println("Document type: " + documentType);
		    		logger.info("Bservices, Document type: " + documentType);
		    		System.out.println("Folder date: " + sDate);
		    		logger.info("Bservices, Folder date: " + sDate);
				}catch (SQLException e){
					logger.error("Bservices ERRO: " + e.getMessage());
					System.out.println("Exception:  " + e.getMessage());
					throw e ;
				} catch (ClassNotFoundException e){
					logger.error("Bservices ERRO: " + e.getMessage());
					System.out.println("Exception:  " + e.getMessage());
					throw e ;
				}
				return result;
			}
			
			private void HandlePdfFile(File relatedExcelFile, String entity, String procNumber, String pdfFilename, String yearMonth, String DestPath, String sDate) throws IOException, SQLException, ClassNotFoundException {
				
				NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(domain_NetworkFolder, user_NetworkFolder,pw_NetworkFolder);
				
				String pdfDir = relatedExcelFile.getParent();
				
				File pdfFile = new File(pdfDir + SMBFileSeparator + pdfFilename);

				String finalPath = DestPath+SMBFileSeparator+entity+SMBFileSeparator+yearMonth+SMBFileSeparator+procNumber;
				String doc_path = "/"+yearMonth +"/"+procNumber;
				SmbFile finalDir  = new SmbFile(finalPath+SMBFileSeparator, auth);
				//File finalDir  = new File(finalPath);
				boolean success = false;
				if (!finalDir.exists()) {
					(new SmbFile(finalPath+SMBFileSeparator,auth)).mkdirs();
					success = true;
				} else {
					success = true;
				}
				if (success){
					copyFiletoEntity(finalPath, pdfFile,DestPath, entity, procNumber, doc_path, sDate);
				}
				
			}
			
			private boolean MakeCsvFile(String sDate, File excelFile, File [] pdffiles, String requerente, String fillingNumber, String authorityProcessIdentification,
					String applicationTypeEnumType, String applicationNumber, String DestPath) throws IOException, SQLException, ClassNotFoundException{
				
				boolean FirstTimeExcelProcessed = true;
				String csvFilename = excelFile.getName().substring(0,excelFile.getName().lastIndexOf('.'))+".csv";
				File csvFile = null ;
				csvFile = new File(csv_temporary_local_dir+File.separator+System.currentTimeMillis()+csvFilename);
				BufferedWriter output = null;
				output = new BufferedWriter(new FileWriter(csvFile));
				output.write("requerente,fillingNumber,authorityProcessIdentification,applicationTypeEnumType,applicationNumber,documentSequence,lastDocumentSequence,filename,comment,documentDetailsEnumtype");
				output.newLine();
				String yearMonth = sDate.substring(0,6);
				String entity = getEntity(requerente);
				String doc_path = "/"+yearMonth +"/"+authorityProcessIdentification;
				for (int i=0; i< pdffiles.length;i++){
					long extension = System.currentTimeMillis();
					insertpdfFileExtension (pdffiles[i].getName(), extension);	
					String rowDataStr = requerente+","+fillingNumber+","+authorityProcessIdentification+","+ applicationTypeEnumType+
								","+applicationNumber+","+i+1+","+pdffiles.length+","+extension+"#"+pdffiles[i].getName()+",,"+"OTHER";
					output.write(rowDataStr);
					output.newLine();
					System.out.println(rowDataStr);
					if (insertMetadataIntoBD(requerente, fillingNumber, authorityProcessIdentification, applicationTypeEnumType,
							applicationNumber,Integer.toString(i+1),Integer.toString(pdffiles.length),pdffiles[i].getName(),"","OTHER",
							entity,doc_path,yearMonth,sDate).equals("OK")){
						
						FirstTimeExcelProcessed = true;
						HandlePdfFile(excelFile, entity, authorityProcessIdentification, pdffiles[i].getName(), yearMonth, DestPath, sDate);

						System.out.println("entity: " +entity + " Process number: " + authorityProcessIdentification);
						logger.info("Bservices entity: " + entity + " Process number: " + authorityProcessIdentification);
						System.out.println("File processed OK: " + pdffiles[i].getName());
						logger.info("Bservices File Processed OK: " + pdffiles[i].getName());

					} else {
						FirstTimeExcelProcessed =false ;
						System.out.println("Este ficheiro excel já foi processado no dia em que foi submetido. Se alterou alguma coisa no excel ou on ofício em pdf, por favor volte a inserir no dia seguinte: "
								+ entity + " Process number: " + authorityProcessIdentification + " filename: " + pdffiles[i].getName());
						logger.info("Bservice: Este ficheiro excel já foi processado no dia em que foi submetido. Se alterou alguma coisa no excel ou on ofício em pdf, por favor volte a inserir no dia seguinte: "
								+ entity + " Process number: " + authorityProcessIdentification + " filename: " + pdffiles[i].getName());
					}
				}
				
				return FirstTimeExcelProcessed;
			}
			
			private boolean isNumeric(String str) { 
				  try {  
				    Double.parseDouble(str);  
				    return true;
				  } catch(NumberFormatException e){  
				    return false;  
				  }  
				}

			private void processExcelFile(File file, String sDate, String DestPath )
					throws UserNotDefinedException, ProcessNumberEmptyException, IOException, SQLException, ClassNotFoundException, FileNotFoundException {

				Workbook workbook = null;
				org.apache.poi.ss.usermodel.Sheet sheet;
				Iterator<Row> rowIterator;
				File outputfile = null ;
				BufferedWriter output = null;
				boolean FirstTimeExcelProcessed = true;

				try {
					File parentDir = file.getParentFile();
					File [] pdffiles=getPdfFilesFromDir(parentDir);
					int nrPdfFiles= pdffiles.length;
					
					FileInputStream fileInputStream;
					outputfile = new File(csv_temporary_local_dir+File.separator+System.currentTimeMillis()+file.getName().substring(0,file.getName().lastIndexOf('.'))+".csv");
					output = new BufferedWriter(new FileWriter(outputfile));

					fileInputStream = new FileInputStream(file);
					String fileExtension = file.getName().substring(file.getName().lastIndexOf("."));
					System.out.println(fileExtension);
					logger.info("Bservices file extension: " + fileExtension);
					if(fileExtension.equals(".xls")){
						workbook  = new HSSFWorkbook(new POIFSFileSystem(fileInputStream));
					}
					else if(fileExtension.equals(".xlsx")){
						workbook  = new XSSFWorkbook(fileInputStream);
					}
					else {
						System.out.println("Wrong File Type");
						logger.info("Bservices: Something is wrong with the Excel file");
					} 
					FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
					sheet = workbook.getSheetAt(0);
					Boolean excelError = false ;
					if (sheet.getLastRowNum() ==0){
						logger.error("Bservices: The excel file does not contain data");
					} else {
						Row row = sheet.getRow(1);
						if (row.getCell(0).getStringCellValue() == null || row.getCell(0).getStringCellValue() == ""){
							excelError = true;
							logger.error("Bservices: Excel file -> requente not defined");
						}
						if (row.getCell(1).getStringCellValue() == null || row.getCell(1).getStringCellValue() == ""){
							excelError = true;
							logger.error("Bservices: Excel file -> fillingNumber not defined");
						}
						if (row.getCell(2).getStringCellValue() == null || row.getCell(2).getStringCellValue() == ""){
							excelError = true;
							logger.error("Bservices: Excel file -> authorityProcessIdentification not defined");
							throw new ProcessNumberEmptyException ("Authority process identification está vazio no ficheiro de excel: ");
						}
						if (row.getCell(3).getStringCellValue() == null || row.getCell(3).getStringCellValue() == ""){
							excelError = true;
							logger.error("Bservices: Excel file -> applicationTypeEnumType not defined");
						}
						try{
							if (row.getCell(4).getStringCellValue() == null || row.getCell(4).getStringCellValue() == ""){
								excelError = true;
								logger.error("Bservices: Excel file -> applicationNumber not defined");
							}else{
								if (!isNumeric(row.getCell(4).getStringCellValue())){
									excelError = true;
									logger.error("Bservices: Excel file -> applicationNumber must be a Number of 14 digits");
								}
							}
						} catch (Exception e){
								logger.info("Bservices: Excel file -> applicationNumber OK");
						}
						if (!excelError){
							String requerente =  row.getCell(0).getStringCellValue();
							checkUsername(requerente);
							String fillingNumber = row.getCell(1).getStringCellValue();
							String authorityProcessIdentification = ReplaceInvalidChars(row.getCell(2).getStringCellValue());
							String applicationTypeEnumType = row.getCell(3).getStringCellValue();
							int applicationNumber = (int)row.getCell(4).getNumericCellValue();
							FirstTimeExcelProcessed = MakeCsvFile(sDate,file, pdffiles, requerente, fillingNumber, authorityProcessIdentification, applicationTypeEnumType,
									Integer.toString(applicationNumber),DestPath);
							
						} else {
							logger.error("Bservices: Excel file -> Ficheiro Excel com dados incompletos");
							System.out.println("Fim do processamento da aplicacao Bservices Answers com ERRO");
							logger.error("Bservices Terminou com ERRO");
						}
					}

				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					logger.error("Bservices ERRO: Ficheiro não encontrado");
					throw e ;
					
				}
				catch (IOException e){
					e.printStackTrace();
					logger.error("Bservices ERRO: problema de IO");
					throw e ;
				}finally {
					if ( output != null ) {
						try{
							output.close();
							if (!FirstTimeExcelProcessed){
								outputfile.delete();
							}
						}
						catch(IOException e){
							e.printStackTrace(); 
							logger.error("Bservices ERRO: problema de IO");
							throw e ;
						}
					}
				}
			}
			
			public ProcessBServicesAnswers (String sDate)
			throws UserNotDefinedException, ProcessNumberEmptyException, IOException, SQLException, ClassNotFoundException, FileNotFoundException, Exception {

				create_BS_directories();
				do_bs_oficios_entidades_externas(ent_externas_path_in + SMBFileSeparator + "BS" + 
						SMBFileSeparator + "BS_ADMINISTRATIVEOFFENCES"+ SMBFileSeparator +sDate, 
						ent_externas_path_out,sDate);
				//Pedido de Informacao de Status Juridico DPI
				do_bs_oficios_entidades_externas(ent_externas_path_in + SMBFileSeparator + "BS" + 
						SMBFileSeparator + "BS_JURIDICALSTATUS"+ SMBFileSeparator +sDate, 
						ent_externas_path_out,sDate);
				//Pedido de Exames Periciais
				do_bs_oficios_entidades_externas(ent_externas_path_in + SMBFileSeparator + "BS" + 
						SMBFileSeparator + "BS_REQUESTEXPERTS"+ SMBFileSeparator +sDate, 
						ent_externas_path_out,sDate);
				//Pedido de Pareceres Tecnico Juridicos
				do_bs_oficios_entidades_externas(ent_externas_path_in + SMBFileSeparator + "BS" + 
						SMBFileSeparator + "BS_JURIDICALADVICES"+ SMBFileSeparator +sDate, 
						ent_externas_path_out,sDate);
				//Averbamentos
				do_bs_oficios_entidades_externas(ent_externas_path_in + SMBFileSeparator + "BS" + 
						SMBFileSeparator + "BS_ANNOTATIONS"+ SMBFileSeparator +sDate, 
						ent_externas_path_out,sDate);
				//Recursos de Deciscoes do INPI
				do_bs_oficios_entidades_externas(ent_externas_path_in + SMBFileSeparator + "BS" + 
						SMBFileSeparator + "BS_APPEALINPIDECISIONS"+ SMBFileSeparator +sDate, 
						ent_externas_path_out,sDate);
				//Sentencas de Tribunal
				do_bs_oficios_entidades_externas(ent_externas_path_in + SMBFileSeparator + "BS" + 
						SMBFileSeparator + "BS_COURTDECISIONS"+ SMBFileSeparator +sDate, 
						ent_externas_path_out, sDate);
				//end inpi_em_rede	

			}
		}
		


		
/*********************************************************************************************************
 * Start Main Program
 *********************************************************************************************************/

		String sDate = null;
		
		try {
			
			Properties props = new Properties();
			readGlobalParameters(props);

			PropertyConfigurator.configure(path_to_log4j_properties);

			System.out.println("starting export Bservices Answers");
			logger.info("Bservices: Início da exportação");


			if (args.length == 0) {
				Calendar cal = Calendar.getInstance();
				SimpleDateFormat sdf = new SimpleDateFormat(MY_DATE_FORMAT);
				cal.getTime();
				cal.add(Calendar.DATE, -1);
				sDate = sdf.format(cal.getTime());
			} else {
				sDate = args[0];
			}
			System.out.println("sDate= " + sDate.toString());

			ProcessBServicesAnswers pbservices = new ProcessBServicesAnswers (sDate) ;
			System.out.println("Fim do processamento da aplicacao Bservices Answers OK");
			logger.info("Bservices terminou OK");

		} catch (UserNotDefinedException e ){
			cleanupFilesAndMetadataWhenCrash(sDate);
			System.out.println("Fim do processamento da aplicacao Bservices Answers com ERRO");
			logger.error("Bservices Terminou com ERRO");
			e.printStackTrace();

		} catch (ProcessNumberEmptyException e ){
			cleanupFilesAndMetadataWhenCrash(sDate);
			System.out.println("Fim do processamento da aplicacao Bservices Answers com ERRO");
			logger.error("Bservices Terminou com ERRO");
			e.printStackTrace();

		} 
		catch (FileNotFoundException e){
			cleanupFilesAndMetadataWhenCrash(sDate);
			System.out.println("Fim do processamento da aplicacao Bservices Answers com ERRO");
			logger.error("Bservices Terminou com ERRO");
			e.printStackTrace();

		} catch (IOException e){
			cleanupFilesAndMetadataWhenCrash(sDate);
			System.out.println("Fim do processamento da aplicacao Bservices Answers com ERRO");
			logger.error("Bservices Terminou com ERRO");
			e.printStackTrace();

		} catch (SQLException e) {
			cleanupFilesAndMetadataWhenCrash(sDate);
			System.out.println("Fim do processamento da aplicacao Bservices Answers com ERRO");
			logger.error("Bservices Terminou com ERRO");
			e.printStackTrace();

		} catch (ClassNotFoundException e){
			cleanupFilesAndMetadataWhenCrash(sDate);
			System.out.println("Fim do processamento da aplicacao Bservices Answers com ERRO");
			logger.error("Bservices Terminou com ERRO");
			e.printStackTrace();

		}catch (Exception e) {
			cleanupFilesAndMetadataWhenCrash(sDate);
			System.out.println("Fim do processamento da aplicacao Bservices Answers com ERRO");
			logger.error("Bservices Terminou com ERRO");
			e.printStackTrace();			
		} finally {
			/* copy all files from csv_temporary_local_dir to mule_fileRepository_csv_path to guaranty that the files are only transferred to mule
			 * if processing the excel files is OK.
			 * If it is not OK csv_temporary_local_dir is empty because it was cleaned by cleanupFilesWhenCrash() method
			 * and nothing is copied.
			 */
			try {
				NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(domain_NetworkFolder, user_NetworkFolder,pw_NetworkFolder);

				copyCsvFromTempToMule(new File(csv_temporary_local_dir), new SmbFile(mule_fileRepository_csv_path));
			} catch (IOException e){
				cleanupFilesAndMetadataWhenCrash(sDate);
				System.out.println("Fim do processamento da aplicacao Bservices Answers com ERRO");
				logger.error("Bservices Terminou com ERRO");
				e.printStackTrace();
			}
		}
	}
}
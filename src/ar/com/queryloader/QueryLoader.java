package ar.com.queryloader;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

public class QueryLoader {
	private Logger logger = Logger.getLogger(QueryLoader.class);
	private HashMap<String, String> queryMap = new HashMap<String, String>();
	

	/**
	 * Public constructor
	 * @param queryFilePath: Location of the file with the queries
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public QueryLoader(String queryFilePath) throws IOException{
		logger.debug("Trying to open the file with the queries. File: "+queryFilePath);
		File queries = new File(queryFilePath);
		if(queries.exists()){
			
			List<String> lines = FileUtils.readLines(queries);
			
			for(int i=0; i<lines.size(); i++){
				String actualLine = lines.get(i);
				if(actualLine.startsWith("--")){
					String queryName = actualLine.substring(2, actualLine.length()).trim();
					String query = "";
					boolean hasOne = false;
					while(i<(lines.size()-1) && !lines.get(i+1).startsWith("--")){//mientras existan lineas siguientes y no empiezen con --
						 query += " " + lines.get(++i);
						 hasOne = true;
					}
					if(hasOne){
						queryMap.put(queryName.toUpperCase(), query);
					}
				}					
			}
		}else
			logger.error("No se encuentra el archivo con los queries. File: "+queryFilePath );
	}

	/**
	 * Returns query map.
	 * @return
	 */
	public HashMap<String, String> getQueryMap() {
		return queryMap;
	}

	/**
	 * Returns query named 'qName'.
	 * @param qName
	 * @return
	 */
	public String getQuery(String qName){
		return queryMap.get(qName.toUpperCase());
	}
	
	/**
	 *
	 * @param queryName
	 * @param c
	 * @param params
	 * @return
	 * @throws QueryLoaderException
	 */
	public ResultSet executeQuery(String queryName, Connection c, Object[] params) throws QueryLoaderException{
		PreparedStatement stmt = null;
		ResultSet RS = null;
		try {
			stmt = c.prepareStatement(queryMap.get(queryName.toUpperCase()));
			ParameterMetaData pmd = stmt.getParameterMetaData();
			//verificamos que coincidan la cantidad de parametros
			if(pmd.getParameterCount() != params.length){
				logger.error("Cantidad de parametros recibidos: "+ params.length +" | Cantidad de parametros esperados: "+pmd.getParameterCount());
				throw new QueryLoaderException("Cantidad de parametros no coincide con los que contiene el query.");
			}
			int i = 0;
			for(Object o : params){
				if(o instanceof String)
					stmt.setString(++i, (String)o);
				else if(o instanceof Integer || o.getClass().toString().equalsIgnoreCase("int"))
					stmt.setInt(++i, (Integer)o);
				else if(o instanceof Blob)
					stmt.setBlob(++i, (Blob)o);
				else if(o instanceof Float || o.getClass().toString().equalsIgnoreCase("float"))
					stmt.setFloat(++i, (Float)o);
				else if(o instanceof BigDecimal)
					stmt.setBigDecimal(++i, (BigDecimal)o);
				else if(o instanceof Boolean || o.getClass().toString().equalsIgnoreCase("boolean"))
					stmt.setBoolean(++i, (Boolean)o);
				else if(o instanceof Long || o.getClass().toString().equalsIgnoreCase("long"))
					stmt.setLong(++i, (Long)o);
				else if(o instanceof Double || o.getClass().toString().equalsIgnoreCase("double"))
					stmt.setDouble(++i, (Double)o);
				else if(o instanceof Date)
					stmt.setDate(++i, new java.sql.Date(((Date)o).getTime()));
				else if(o instanceof java.sql.Date)
					stmt.setDate(++i, (java.sql.Date)o);
				else if(o instanceof Short || o.getClass().toString().equalsIgnoreCase("short"))
					stmt.setShort(++i, (Short)o);
				else if(o instanceof Time)
					stmt.setTime(++i, (Time)o);
				else if(o instanceof Timestamp)
					stmt.setTimestamp(++i, (Timestamp)o);
				else
					stmt.setObject(++i, o);
			}
			RS = stmt.executeQuery();
			return RS;
		} catch (SQLException e) {
			throw new QueryLoaderException("Ocurrio una excepción de SQL al intentar completar el statement. Cause:"+e.toString(), e);
		}finally{
			DbUtils.closeQuietly(stmt);
			DbUtils.closeQuietly(c);
		}
	}

	
		
}

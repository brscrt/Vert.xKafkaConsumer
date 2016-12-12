package util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MyProperties extends Properties{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public void loadProperties(String filePath){
		InputStream input=null;
		try {
			input = new FileInputStream(filePath);
			load(input);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}

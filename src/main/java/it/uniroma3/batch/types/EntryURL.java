package it.uniroma3.batch.types;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;

import org.bson.Document;

public class EntryURL implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 8915130542970299822L;
	private String url;
	private Date latestUpdate;
	private String snapshot;
	
	public EntryURL(Document doc) {		
		doc.entrySet().forEach(entry -> {
			if(!Arrays.asList(this.getClass().getDeclaredFields()).stream().map(field -> field.getName()).collect(Collectors.toList()).contains(entry.getKey().toString()))
				return;
			
			Field field = null;
			try {
				field = this.getClass().getDeclaredField(entry.getKey());
				boolean isAccesible = field.isAccessible();
				field.setAccessible(true);
				
				field.set(this, entry.getValue());
				field.setAccessible(isAccesible);
				
			} catch (NoSuchFieldException 
					| SecurityException 
					| IllegalArgumentException 
					| IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			
		});
	}

	public String getUrl() {
		return url;
	}

	public Date getLatestUpdate() {
		return latestUpdate;
	}

	public String getSnapshot() {
		return snapshot;
	}

	public String toString() {
		return this.url;
	}

}

package it.anitia.batch.googledrive.reorganize;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Settings {

	final static Logger logger = LogManager.getLogger(Settings.class);

	public static String serviceAccountKeyFile = "config/upload-gdrive-443816-e667cf3f212b.json";

	public static class operation {
		public static int retry = 3;
		public static int sleepRetry = 10;
	}

	public static class folder {
		public static class source {
			public static String id;
			public static boolean recursive = true;
		}
		public static class destination {
			public static String id;
		}
	}

	private Settings() {
		throw new IllegalStateException("Settings class");
	}

	public static void initialize() throws Exception {
		File file = null;
		String propertiesFile = "config/googledrivereorganize.properties";

		// System properties override
		if (System.getProperty("googledrivereorganize.serviceAccountKeyFile") != null)
			serviceAccountKeyFile = System.getProperty("googledrivereorganize.serviceAccountKeyFile");

		if (System.getProperty("googledrivereorganize.operation.retry") != null)
			operation.retry = Integer.parseInt(System.getProperty("googledrivereorganize.operation.retry"));
		if (System.getProperty("googledrivereorganize.operation.sleepRetry") != null)
			operation.sleepRetry = Integer.parseInt(System.getProperty("googledrivereorganize.operation.sleepRetry"));

		if (System.getProperty("googledrivereorganize.folder.source.id") != null)
			folder.source.id = System.getProperty("googledrivereorganize.folder.source.id");
		if (System.getProperty("googledrivereorganize.folder.source.recursive") != null)
			folder.source.recursive = Boolean.parseBoolean(System.getProperty("googledrivereorganize.folder.source.recursive"));

		if (System.getProperty("googledrivereorganize.folder.destination.id") != null)
			folder.destination.id = System.getProperty("googledrivereorganize.folder.destination.id");

		file = new File(propertiesFile);

		if (file.exists()) {
			try (FileInputStream fis = new FileInputStream(file)) {
				Properties properties = new Properties();
				properties.load(fis);

				if (properties.containsKey("serviceAccountKeyFile"))
					serviceAccountKeyFile = properties.get("serviceAccountKeyFile").toString();

				if (properties.containsKey("operation.retry"))
					operation.retry = Integer.parseInt(properties.get("operation.retry").toString());
				if (properties.containsKey("operation.sleepRetry"))
					operation.sleepRetry = Integer.parseInt(properties.get("operation.sleepRetry").toString());

				if (properties.containsKey("folder.source.id"))
					folder.source.id = properties.get("folder.source.id").toString();
				if (properties.containsKey("folder.source.recursive"))
					folder.source.recursive = Boolean.parseBoolean(properties.get("folder.source.recursive").toString());

				if (properties.containsKey("folder.destination.id"))
					folder.destination.id = properties.get("folder.destination.id").toString();
			}
			catch (IOException e) {
				throw e;
			}
		}

		logger.info("serviceAccountKeyFile........: '{}'", serviceAccountKeyFile);
		logger.info("operation.retry..............: {}", operation.retry);
		logger.info("operation.sleepRetry.........: {}", operation.sleepRetry);
		logger.info("folder.source.id.............: '{}'", folder.source.id);
		logger.info("folder.source.recursive......: {}", folder.source.recursive);
		logger.info("folder.destination.id........: '{}'", folder.destination.id);

		if (null == folder.source.id || folder.source.id.isBlank() || folder.source.id.isEmpty())
			throw new Exception("configurazione \"folder.source.id\" assente");

		if (null == folder.destination.id || folder.destination.id.isBlank() || folder.destination.id.isEmpty())
			throw new Exception("configurazione \"folder.destination.id\" assente");

		file = new File(serviceAccountKeyFile);
		if (!file.exists())
			throw new Exception(String.format("Service Account Key File \"%s\" non trovato", serviceAccountKeyFile));
	}
}

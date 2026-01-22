package it.anitia.batch.googledrive.reorganize;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

public class App {

	final static Logger logger = LogManager.getLogger(App.class);
	private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
	private static final String FOLDER_MIME_TYPE = "application/vnd.google-apps.folder";

	private static boolean executeReorganize = false;
	private static boolean executeList = false;
	private static boolean dryRun = false;

	private static Map<String, String> folderCache = new HashMap<>();
	private static int filesProcessed = 0;
	private static int filesMoved = 0;
	private static int filesError = 0;

	public static void main(String[] args) {
		if (!checkArgs(args)) {
			return;
		}

		logger.info("INIZIO Google Drive Reorganize");
		logger.trace("Lettura configurazione");

		try {
			Settings.initialize();
		} catch (Exception e) {
			logger.fatal("Errore di configurazione: {}", e.getMessage());
			System.exit(-1);
		}

		try {
			Drive driveService = getDriveService();

			if (executeList) {
				logger.info("=== Modalita' LIST ===");
				listFolderContents(driveService, Settings.folder.source.id, "", Settings.folder.source.recursive);
			} else if (executeReorganize) {
				logger.info("=== Modalita' REORGANIZE {} ===", dryRun ? "(DRY RUN)" : "");
				reorganizeFolder(driveService, Settings.folder.source.id, "");
				logger.info("=== RIEPILOGO ===");
				logger.info("File elaborati: {}", filesProcessed);
				logger.info("File spostati:  {}", filesMoved);
				logger.info("File in errore: {}", filesError);
			}
		} catch (IOException e) {
			logger.fatal("Eccezione {}: {}", e.getClass().getName(), e.getMessage());
		}

		logger.info("FINE Google Drive Reorganize");
	}

	private static boolean checkArgs(String[] args) {
		if (args.length < 1) {
			logger.fatal("Uso corretto: java -jar googledrivereorganize.jar [-r|-l] [-dry]");
			logger.fatal("  -r   : Reorganize - sposta i file nella struttura ANNO/MESE");
			logger.fatal("  -l   : List - elenca il contenuto della cartella sorgente");
			logger.fatal("  -dry : Dry run - simula le operazioni senza modificare nulla");
			return false;
		}

		for (String arg : args) {
			if (arg.equals("-r")) {
				executeReorganize = true;
			} else if (arg.equals("-l")) {
				executeList = true;
			} else if (arg.equals("-dry")) {
				dryRun = true;
			}
		}

		if (!executeReorganize && !executeList) {
			logger.fatal("Errore: specificare -r (reorganize) o -l (list)");
			return false;
		}

		if (executeReorganize && executeList) {
			logger.fatal("Errore: specificare solo -r o -l, non entrambi");
			return false;
		}

		return true;
	}

	public static Drive getDriveService() throws IOException {
		GoogleCredentials credentials = GoogleCredentials.fromStream(
				java.nio.file.Files.newInputStream(Paths.get(Settings.serviceAccountKeyFile)))
				.createScoped(Collections.singleton(DriveScopes.DRIVE));

		return new Drive.Builder(new NetHttpTransport(), JSON_FACTORY, new HttpCredentialsAdapter(credentials))
				.setApplicationName("Drive API Java Reorganize")
				.build();
	}

	public static void listFolderContents(Drive service, String folderId, String indent, boolean recursive) throws IOException {
		// List files in folder
		String fileQuery = String.format("'%s' in parents and mimeType!='%s' and trashed=false", folderId, FOLDER_MIME_TYPE);
		FileList fileResult = service.files().list()
				.setQ(fileQuery)
				.setSpaces("drive")
				.setFields("files(id, name, mimeType, modifiedTime, size)")
				.setSupportsAllDrives(true)
				.setIncludeItemsFromAllDrives(true)
				.execute();

		List<File> files = fileResult.getFiles();
		if (files != null && !files.isEmpty()) {
			for (File file : files) {
				String[] yearMonth = getYearMonthFromGDriveFile(file);
				logger.info("{}- [FILE] {} (ID: {}, Modified: {}/{}, Size: {})",
						indent, file.getName(), file.getId(), yearMonth[0], yearMonth[1],
						file.getSize() != null ? file.getSize() : "N/A");
			}
		}

		// List subfolders
		String folderQuery = String.format("'%s' in parents and mimeType='%s' and trashed=false", folderId, FOLDER_MIME_TYPE);
		FileList folderResult = service.files().list()
				.setQ(folderQuery)
				.setSpaces("drive")
				.setFields("files(id, name)")
				.setSupportsAllDrives(true)
				.setIncludeItemsFromAllDrives(true)
				.execute();

		List<File> folders = folderResult.getFiles();
		if (folders != null && !folders.isEmpty()) {
			for (File folder : folders) {
				logger.info("{}- [DIR]  {} (ID: {})", indent, folder.getName(), folder.getId());
				if (recursive) {
					listFolderContents(service, folder.getId(), indent + "   ", recursive);
				}
			}
		}

		if ((files == null || files.isEmpty()) && (folders == null || folders.isEmpty())) {
			logger.info("{} (vuoto)", indent);
		}
	}

	public static void reorganizeFolder(Drive service, String folderId, String relativePath) throws IOException {
		// Process files in current folder
		String fileQuery = String.format("'%s' in parents and mimeType!='%s' and trashed=false", folderId, FOLDER_MIME_TYPE);
		FileList fileResult = service.files().list()
				.setQ(fileQuery)
				.setSpaces("drive")
				.setFields("files(id, name, mimeType, modifiedTime, size)")
				.setSupportsAllDrives(true)
				.setIncludeItemsFromAllDrives(true)
				.execute();

		List<File> files = fileResult.getFiles();
		if (files != null && !files.isEmpty()) {
			for (File file : files) {
				processFile(service, file, relativePath);
			}
		}

		// Process subfolders if recursive
		if (Settings.folder.source.recursive) {
			String folderQuery = String.format("'%s' in parents and mimeType='%s' and trashed=false", folderId, FOLDER_MIME_TYPE);
			FileList folderResult = service.files().list()
					.setQ(folderQuery)
					.setSpaces("drive")
					.setFields("files(id, name)")
					.setSupportsAllDrives(true)
					.setIncludeItemsFromAllDrives(true)
					.execute();

			List<File> folders = folderResult.getFiles();
			if (folders != null && !folders.isEmpty()) {
				for (File folder : folders) {
					// Skip year/month folders to avoid infinite recursion when source = destination
					if (isYearFolder(folder.getName()) || isMonthFolder(folder.getName())) {
						logger.debug("Cartella \"{}\" ignorata (struttura ANNO/MESE)", folder.getName());
						continue;
					}
					String newRelativePath = relativePath.isEmpty() ? folder.getName() : relativePath + "/" + folder.getName();
					reorganizeFolder(service, folder.getId(), newRelativePath);
				}
			}
		}
	}

	private static void processFile(Drive service, File file, String relativePath) {
		filesProcessed++;
		String fileName = file.getName();

		// Extract year/month from modifiedTime
		String[] yearMonth = getYearMonthFromGDriveFile(file);
		String year = yearMonth[0];
		String month = yearMonth[1];

		// Build destination path: relativePath/ANNO/MESE
		List<String> pathSegments = new ArrayList<>();
		if (relativePath != null && !relativePath.isEmpty()) {
			for (String segment : relativePath.split("/")) {
				if (!segment.isEmpty() && !".".equals(segment) && !"..".equals(segment)) {
					pathSegments.add(segment);
				}
			}
		}
		pathSegments.add(year);
		pathSegments.add(month);

		String destinationPath = String.join("/", pathSegments);
		logger.info("Elaborazione file \"{}\" -> {}/{}", fileName, destinationPath, fileName);

		try {
			// Ensure remote path exists
			String targetFolderId = ensureRemotePath(service, Settings.folder.destination.id, pathSegments);

			// Resolve conflicts
			String finalFileName = resolveConflict(service, targetFolderId, fileName);

			// Move file
			if (dryRun) {
				logger.info("[DRY RUN] Spostamento file \"{}\" in {} come \"{}\"", fileName, destinationPath, finalFileName);
				filesMoved++;
			} else {
				moveFile(service, file.getId(), targetFolderId, finalFileName);
				logger.info("File \"{}\" spostato in {} come \"{}\"", fileName, destinationPath, finalFileName);
				filesMoved++;
			}
		} catch (IOException e) {
			logger.error("Errore durante l'elaborazione del file \"{}\": {}", fileName, e.getMessage());
			filesError++;
		}
	}

	private static boolean isYearFolder(String name) {
		if (name == null || name.length() != 4) {
			return false;
		}
		try {
			int year = Integer.parseInt(name);
			return year >= 1900 && year <= 2100;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	private static boolean isMonthFolder(String name) {
		if (name == null || name.length() != 2) {
			return false;
		}
		try {
			int month = Integer.parseInt(name);
			return month >= 1 && month <= 12;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	private static String[] getYearMonthFromGDriveFile(File file) {
		DateTime modifiedTime = file.getModifiedTime();
		if (modifiedTime == null) {
			LocalDate now = LocalDate.now();
			return new String[]{String.valueOf(now.getYear()), String.format("%02d", now.getMonthValue())};
		}

		long timestamp = modifiedTime.getValue();
		Instant instant = Instant.ofEpochMilli(timestamp);
		LocalDate date = instant.atZone(ZoneId.systemDefault()).toLocalDate();
		String year = String.valueOf(date.getYear());
		String month = String.format("%02d", date.getMonthValue());
		return new String[]{year, month};
	}

	private static String ensureRemotePath(Drive service, String startFolderId, List<String> segments) throws IOException {
		String currentFolderId = startFolderId;
		for (String segment : segments) {
			if (segment == null || segment.isBlank() || ".".equals(segment) || "..".equals(segment)) {
				continue;
			}
			currentFolderId = createFolderIfNotExists(service, currentFolderId, segment);
		}
		return currentFolderId;
	}

	private static String createFolderIfNotExists(Drive service, String parentId, String folderName) throws IOException {
		String cacheKey = parentId + "/" + folderName;
		if (folderCache.containsKey(cacheKey)) {
			return folderCache.get(cacheKey);
		}

		// In dry run mode with fake parent, skip API call and return fake ID
		if (dryRun && parentId.startsWith("dryrun-")) {
			logger.info("[DRY RUN] Creazione cartella \"{}\" in {}", folderName, parentId);
			String fakeFolderId = "dryrun-" + cacheKey.hashCode();
			folderCache.put(cacheKey, fakeFolderId);
			return fakeFolderId;
		}

		String query = String.format("name='%s' and '%s' in parents and mimeType='%s' and trashed=false",
				folderName, parentId, FOLDER_MIME_TYPE);
		Drive.Files.List request = service.files().list()
				.setQ(query)
				.setSpaces("drive")
				.setFields("files(id)")
				.setIncludeItemsFromAllDrives(true)
				.setSupportsAllDrives(true);

		List<File> files = request.execute().getFiles();
		if (!files.isEmpty()) {
			String folderId = files.get(0).getId();
			folderCache.put(cacheKey, folderId);
			return folderId;
		}

		if (dryRun) {
			logger.info("[DRY RUN] Creazione cartella \"{}\" in {}", folderName, parentId);
			String fakeFolderId = "dryrun-" + cacheKey.hashCode();
			folderCache.put(cacheKey, fakeFolderId);
			return fakeFolderId;
		}

		File folderMetadata = new File();
		folderMetadata.setName(folderName);
		folderMetadata.setMimeType(FOLDER_MIME_TYPE);
		folderMetadata.setParents(Collections.singletonList(parentId));

		File folder = service.files().create(folderMetadata)
				.setFields("id")
				.setSupportsAllDrives(true)
				.execute();

		logger.debug("Cartella \"{}\" creata con ID: {}", folderName, folder.getId());
		folderCache.put(cacheKey, folder.getId());
		return folder.getId();
	}

	private static String resolveConflict(Drive service, String folderId, String fileName) throws IOException {
		String baseName = fileName;
		String extension = "";
		int lastDot = fileName.lastIndexOf('.');
		if (lastDot > 0) {
			baseName = fileName.substring(0, lastDot);
			extension = fileName.substring(lastDot);
		}

		String currentName = fileName;
		int counter = 0;

		while (fileExistsInFolder(service, folderId, currentName)) {
			counter++;
			currentName = baseName + "_" + counter + extension;
			logger.debug("Conflitto rilevato, tentativo con nome: {}", currentName);
		}

		if (counter > 0) {
			logger.info("File \"{}\" rinominato in \"{}\" per evitare conflitto", fileName, currentName);
		}

		return currentName;
	}

	private static boolean fileExistsInFolder(Drive service, String folderId, String fileName) throws IOException {
		if (dryRun && folderId.startsWith("dryrun-")) {
			return false;
		}

		String query = String.format("name='%s' and '%s' in parents and trashed=false", fileName, folderId);
		FileList result = service.files().list()
				.setQ(query)
				.setSpaces("drive")
				.setFields("files(id)")
				.setSupportsAllDrives(true)
				.setIncludeItemsFromAllDrives(true)
				.execute();

		return result.getFiles() != null && !result.getFiles().isEmpty();
	}

	private static void moveFile(Drive service, String fileId, String targetFolderId, String newFileName) throws IOException {
		int retry = 0;
		boolean moveDone = false;

		while (retry < Settings.operation.retry && !moveDone) {
			try {
				retry++;
				logger.debug("Tentativo {}/{} di spostamento file", retry, Settings.operation.retry);

				// Get current parents
				File file = service.files().get(fileId)
						.setFields("parents")
						.setSupportsAllDrives(true)
						.execute();

				StringBuilder previousParents = new StringBuilder();
				if (file.getParents() != null) {
					for (String parent : file.getParents()) {
						if (previousParents.length() > 0) {
							previousParents.append(',');
						}
						previousParents.append(parent);
					}
				}

				// Update file: move to new parent and optionally rename
				File updateMetadata = new File();
				updateMetadata.setName(newFileName);

				service.files().update(fileId, updateMetadata)
						.setAddParents(targetFolderId)
						.setRemoveParents(previousParents.toString())
						.setSupportsAllDrives(true)
						.setFields("id, parents")
						.execute();

				moveDone = true;
			} catch (IOException e) {
				logger.warn("Tentativo {}/{} di spostamento fallito: {}", retry, Settings.operation.retry, e.getMessage());
				if (retry >= Settings.operation.retry) {
					throw e;
				}
				logger.info("Nuovo tentativo tra {} secondi", Settings.operation.sleepRetry);
				try {
					Thread.sleep(Settings.operation.sleepRetry * 1000L);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					logger.error("Sleep interrotto: {}", ie.getMessage());
				}
			}
		}
	}
}

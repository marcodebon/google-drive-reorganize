package it.anitia.batch.googledrive.reorganize;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
	// Pattern per riconoscere date nel nome file
	// 1) YYYY-MM-DD o YYYY_MM_DD (con separatori)
	private static final Pattern DATE_PATTERN = Pattern.compile("(\\d{4})[-_](\\d{2})[-_](\\d{2})");
	// 2) YYYYMMDD compatto (8 cifre consecutive, anche precedute da D come in CAMS)
	private static final Pattern DATE_COMPACT_PATTERN = Pattern.compile("D?(\\d{4})(\\d{2})(\\d{2})");
	// 3) PDF provincia: XX-NN-ANNO-SEQ dove ANNO e' 2 o 4 cifre
	private static final Pattern PROVINCE_PDF_PATTERN = Pattern.compile("^[A-Z]{2}-\\d{2}-(\\d{2,4})-\\d+\\.pdf$");

	private static boolean executeReorganize = false;
	private static boolean executeList = false;
	private static boolean executeAnalyze = false;
	private static boolean executeRecover = false;
	private static boolean executeGlacier = false;
	private static String glacierUntilYearMonth = null;
	private static boolean dryRun = false;

	private static Map<String, String> folderCache = new ConcurrentHashMap<>();
	private static AtomicInteger filesProcessed = new AtomicInteger(0);
	private static AtomicInteger filesMoved = new AtomicInteger(0);
	private static AtomicInteger filesError = new AtomicInteger(0);
	private static AtomicInteger glacierFilesArchived = new AtomicInteger(0);
	private static AtomicInteger glacierZipsCreated = new AtomicInteger(0);
	private static ExecutorService executorService;

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

			if (executeAnalyze) {
				logger.info("=== Modalita' ANALYZE ===");
				analyzeFolder(driveService, Settings.folder.source.id, Settings.folder.source.recursive);
			} else if (executeList) {
				logger.info("=== Modalita' LIST ===");
				listFolderContents(driveService, Settings.folder.source.id, "", Settings.folder.source.recursive);
			} else if (executeRecover) {
				logger.info("=== Modalita' RECOVER ===");
				recoverFiles(driveService, Settings.folder.source.id, Settings.folder.source.recursive);
			} else if (executeReorganize) {
				logger.info("=== Modalita' REORGANIZE {} (maxThreads={}) ===",
						dryRun ? "(DRY RUN)" : "", Settings.operation.maxThreads);
				executorService = Executors.newFixedThreadPool(Settings.operation.maxThreads);
				reorganizeFolder(driveService, Settings.folder.source.id, "");
				executorService.shutdown();
				try {
					executorService.awaitTermination(24, TimeUnit.HOURS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					logger.error("Attesa completamento thread interrotta");
				}
				// Pulizia cartelle vuote nella source
				logger.info("=== Pulizia cartelle vuote ===");
				int foldersDeleted = deleteEmptyFolders(driveService, Settings.folder.source.id, Settings.folder.source.recursive);
				logger.info("=== RIEPILOGO ===");
				logger.info("File elaborati:     {}", filesProcessed.get());
				logger.info("File spostati:      {}", filesMoved.get());
				logger.info("File in errore:     {}", filesError.get());
				logger.info("Cartelle eliminate: {}", foldersDeleted);
			} else if (executeGlacier) {
				if (Settings.folder.glacier.id == null || Settings.folder.glacier.id.isBlank()) {
					logger.fatal("Configurazione \"folder.glacier.id\" assente, necessaria per la modalita' glacier");
					System.exit(-1);
				}
				logger.info("=== Modalita' GLACIER fino a {} (maxZipSizeMB={}) ===",
						glacierUntilYearMonth, Settings.glacier.maxZipSizeMB);
				glacierFolder(driveService, Settings.folder.source.id, "", glacierUntilYearMonth);
				logger.info("=== Pulizia cartelle vuote ===");
				int foldersDeleted = deleteEmptyFolders(driveService, Settings.folder.source.id, true);
				logger.info("=== RIEPILOGO GLACIER ===");
				logger.info("File archiviati:    {}", glacierFilesArchived.get());
				logger.info("ZIP creati:         {}", glacierZipsCreated.get());
				logger.info("Cartelle eliminate: {}", foldersDeleted);
			}
		} catch (IOException e) {
			logger.fatal("Eccezione {}: {}", e.getClass().getName(), e.getMessage());
		}

		logger.info("FINE Google Drive Reorganize");
	}

	private static boolean checkArgs(String[] args) {
		if (args.length < 1) {
			logger.fatal("Uso corretto: java -jar googledrivereorganize.jar [-r|-l|-a|-rec|-g YYYY-MM] [-dry]");
			logger.fatal("  -r          : Reorganize - sposta i file nella struttura ANNO/MESE");
			logger.fatal("  -l          : List - elenca il contenuto della cartella sorgente");
			logger.fatal("  -a          : Analyze - analizza i pattern dei nomi file");
			logger.fatal("  -rec        : Recover - scarica i file elencati in torecover.txt");
			logger.fatal("  -g YYYY-MM  : Glacier - archivia in ZIP i file con data <= YYYY-MM");
			logger.fatal("  -dry        : Dry run - simula le operazioni senza modificare nulla");
			return false;
		}

		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			if (arg.equals("-r")) {
				executeReorganize = true;
			} else if (arg.equals("-l")) {
				executeList = true;
			} else if (arg.equals("-a")) {
				executeAnalyze = true;
			} else if (arg.equals("-rec")) {
				executeRecover = true;
			} else if (arg.equals("-g")) {
				executeGlacier = true;
				if (i + 1 >= args.length) {
					logger.fatal("Errore: -g richiede un argomento YYYY-MM");
					return false;
				}
				glacierUntilYearMonth = args[++i];
				if (!glacierUntilYearMonth.matches("^\\d{4}-\\d{2}$")) {
					logger.fatal("Errore: formato YYYY-MM non valido: {}", glacierUntilYearMonth);
					return false;
				}
			} else if (arg.equals("-dry")) {
				dryRun = true;
			}
		}

		if (!executeReorganize && !executeList && !executeAnalyze && !executeRecover && !executeGlacier) {
			logger.fatal("Errore: specificare -r (reorganize), -l (list), -a (analyze), -rec (recover) o -g (glacier)");
			return false;
		}

		int modeCount = (executeReorganize ? 1 : 0) + (executeList ? 1 : 0) + (executeAnalyze ? 1 : 0)
				+ (executeRecover ? 1 : 0) + (executeGlacier ? 1 : 0);
		if (modeCount > 1) {
			logger.fatal("Errore: specificare solo una modalita' tra -r, -l, -a, -rec e -g");
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
		// List files in folder (con paginazione)
		boolean hasFiles = false;
		String pageToken = null;
		do {
			String fileQuery = String.format("'%s' in parents and mimeType!='%s' and trashed=false", folderId, FOLDER_MIME_TYPE);
			FileList fileResult = service.files().list()
					.setQ(fileQuery)
					.setSpaces("drive")
					.setFields("nextPageToken, files(id, name, mimeType, modifiedTime, size)")
					.setPageToken(pageToken)
					.setSupportsAllDrives(true)
					.setIncludeItemsFromAllDrives(true)
					.execute();

			List<File> files = fileResult.getFiles();
			if (files != null && !files.isEmpty()) {
				hasFiles = true;
				for (File file : files) {
					String[] yearMonth = getYearMonthFromGDriveFile(file);
					logger.info("{}- [FILE] {} (ID: {}, Modified: {}/{}, Size: {})",
							indent, file.getName(), file.getId(), yearMonth[0], yearMonth[1],
							file.getSize() != null ? file.getSize() : "N/A");
				}
			}
			pageToken = fileResult.getNextPageToken();
		} while (pageToken != null);

		// List subfolders (con paginazione)
		boolean hasFolders = false;
		pageToken = null;
		do {
			String folderQuery = String.format("'%s' in parents and mimeType='%s' and trashed=false", folderId, FOLDER_MIME_TYPE);
			FileList folderResult = service.files().list()
					.setQ(folderQuery)
					.setSpaces("drive")
					.setFields("nextPageToken, files(id, name)")
					.setPageToken(pageToken)
					.setSupportsAllDrives(true)
					.setIncludeItemsFromAllDrives(true)
					.execute();

			List<File> folders = folderResult.getFiles();
			if (folders != null && !folders.isEmpty()) {
				hasFolders = true;
				for (File folder : folders) {
					logger.info("{}- [DIR]  {} (ID: {})", indent, folder.getName(), folder.getId());
					if (recursive) {
						listFolderContents(service, folder.getId(), indent + "   ", recursive);
					}
				}
			}
			pageToken = folderResult.getNextPageToken();
		} while (pageToken != null);

		if (!hasFiles && !hasFolders) {
			logger.info("{} (vuoto)", indent);
		}
	}

	public static void reorganizeFolder(Drive service, String folderId, String relativePath) throws IOException {
		// Process files in current folder (con paginazione)
		String pageToken = null;
		do {
			String fileQuery = String.format("'%s' in parents and mimeType!='%s' and trashed=false", folderId, FOLDER_MIME_TYPE);
			FileList fileResult = service.files().list()
					.setQ(fileQuery)
					.setSpaces("drive")
					.setFields("nextPageToken, files(id, name, mimeType, modifiedTime, size)")
					.setPageToken(pageToken)
					.setSupportsAllDrives(true)
					.setIncludeItemsFromAllDrives(true)
					.execute();

			List<File> files = fileResult.getFiles();
			if (files != null && !files.isEmpty()) {
				for (File file : files) {
					final String rp = relativePath;
					executorService.submit(() -> processFile(service, file, rp));
				}
			}
			pageToken = fileResult.getNextPageToken();
		} while (pageToken != null);

		// Process subfolders if recursive (con paginazione)
		if (Settings.folder.source.recursive) {
			pageToken = null;
			do {
				String folderQuery = String.format("'%s' in parents and mimeType='%s' and trashed=false", folderId, FOLDER_MIME_TYPE);
				FileList folderResult = service.files().list()
						.setQ(folderQuery)
						.setSpaces("drive")
						.setFields("nextPageToken, files(id, name)")
						.setPageToken(pageToken)
						.setSupportsAllDrives(true)
						.setIncludeItemsFromAllDrives(true)
						.execute();

				List<File> folders = folderResult.getFiles();
				if (folders != null && !folders.isEmpty()) {
					for (File folder : folders) {
						String folderName = folder.getName();
						if (isYearFolder(folderName)) {
							// Entro nella cartella anno ma non la aggiungo al relativePath
							logger.debug("Entro nella cartella anno \"{}\" (non aggiunta al relativePath)", folderName);
							reorganizeFolder(service, folder.getId(), relativePath);
						} else if (isMonthFolder(folderName)) {
							// Entro nella cartella mese ma non la aggiungo al relativePath
							logger.debug("Entro nella cartella mese \"{}\" (non aggiunta al relativePath)", folderName);
							reorganizeFolder(service, folder.getId(), relativePath);
						} else {
							String newRelativePath = relativePath.isEmpty() ? folderName : relativePath + "/" + folderName;
							reorganizeFolder(service, folder.getId(), newRelativePath);
						}
					}
				}
				pageToken = folderResult.getNextPageToken();
			} while (pageToken != null);
		}
	}

	private static void processFile(Drive service, File file, String relativePath) {
		filesProcessed.incrementAndGet();
		String fileName = file.getName();

		// Extract year/month from modifiedTime
		String[] yearMonth = getYearMonthFromGDriveFile(file);
		String year = yearMonth[0];
		String month = yearMonth[1];

		// Build destination path: ANNO/MESE/relativePath (YYYY/MM in testa)
		List<String> pathSegments = new ArrayList<>();
		pathSegments.add(year);
		pathSegments.add(month);
		if (relativePath != null && !relativePath.isEmpty()) {
			for (String segment : relativePath.split("/")) {
				if (!segment.isEmpty() && !".".equals(segment) && !"..".equals(segment)) {
					pathSegments.add(segment);
				}
			}
		}

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
				filesMoved.incrementAndGet();
			} else {
				moveFile(service, file.getId(), targetFolderId, finalFileName);
				logger.info("File \"{}\" spostato in {} come \"{}\"", fileName, destinationPath, finalFileName);
				filesMoved.incrementAndGet();
			}
		} catch (IOException e) {
			logger.error("Errore durante l'elaborazione del file \"{}\": {}", fileName, e.getMessage());
			filesError.incrementAndGet();
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
		// Prima cerca una data nel nome del file
		String[] fromName = extractDateFromFileName(file.getName());
		if (fromName != null && !"00".equals(fromName[1])) {
			// Anno e mese completi dal nome
			logger.debug("Data estratta dal nome file \"{}\": {}/{}", file.getName(), fromName[0], fromName[1]);
			return fromName;
		}

		if (fromName != null && "00".equals(fromName[1])) {
			// Anno dal nome, mese dal modifiedTime
			String year = fromName[0];
			DateTime modifiedTime = file.getModifiedTime();
			if (modifiedTime != null) {
				long timestamp = modifiedTime.getValue();
				Instant instant = Instant.ofEpochMilli(timestamp);
				LocalDate date = instant.atZone(ZoneId.systemDefault()).toLocalDate();
				String month = String.format("%02d", date.getMonthValue());
				logger.debug("File \"{}\": anno {} dal nome, mese {} da modifiedTime", file.getName(), year, month);
				return new String[]{year, month};
			}
			// Nessun modifiedTime, usa mese 01 come fallback
			logger.debug("File \"{}\": anno {} dal nome, mese non disponibile, uso 01", file.getName(), year);
			return new String[]{year, "01"};
		}

		// Fallback: usa modifiedTime
		DateTime modifiedTime = file.getModifiedTime();
		if (modifiedTime == null) {
			LocalDate now = LocalDate.now();
			logger.debug("File \"{}\" senza data nel nome e senza modifiedTime, uso data odierna", file.getName());
			return new String[]{String.valueOf(now.getYear()), String.format("%02d", now.getMonthValue())};
		}

		long timestamp = modifiedTime.getValue();
		Instant instant = Instant.ofEpochMilli(timestamp);
		LocalDate date = instant.atZone(ZoneId.systemDefault()).toLocalDate();
		String year = String.valueOf(date.getYear());
		String month = String.format("%02d", date.getMonthValue());
		logger.debug("File \"{}\" senza data nel nome, uso modifiedTime: {}/{}", file.getName(), year, month);
		return new String[]{year, month};
	}

	private static String[] extractDateFromFileName(String fileName) {
		if (fileName == null) {
			return null;
		}

		// 1) YYYY-MM-DD o YYYY_MM_DD
		Matcher matcher = DATE_PATTERN.matcher(fileName);
		if (matcher.find()) {
			int y = Integer.parseInt(matcher.group(1));
			int m = Integer.parseInt(matcher.group(2));
			if (y >= 1900 && y <= 2100 && m >= 1 && m <= 12) {
				return new String[]{matcher.group(1), matcher.group(2)};
			}
		}

		// 2) YYYYMMDD compatto (anche con prefisso D per CAMS)
		Matcher compactMatcher = DATE_COMPACT_PATTERN.matcher(fileName);
		if (compactMatcher.find()) {
			int y = Integer.parseInt(compactMatcher.group(1));
			int m = Integer.parseInt(compactMatcher.group(2));
			int d = Integer.parseInt(compactMatcher.group(3));
			if (y >= 1900 && y <= 2100 && m >= 1 && m <= 12 && d >= 1 && d <= 31) {
				return new String[]{compactMatcher.group(1), compactMatcher.group(2)};
			}
		}

		// 3) PDF provincia: XX-NN-ANNO-SEQ.pdf (solo anno, mese non disponibile)
		Matcher provinceMatcher = PROVINCE_PDF_PATTERN.matcher(fileName);
		if (provinceMatcher.find()) {
			String yearStr = provinceMatcher.group(1);
			int y;
			if (yearStr.length() == 2) {
				y = 2000 + Integer.parseInt(yearStr);
				yearStr = String.valueOf(y);
			} else {
				y = Integer.parseInt(yearStr);
			}
			if (y >= 2000 && y <= 2100) {
				// Nessun mese disponibile, ritorna "00" come segnale
				return new String[]{yearStr, "00"};
			}
		}

		return null;
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

	private static synchronized String createFolderIfNotExists(Drive service, String parentId, String folderName) throws IOException {
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

	public static void analyzeFolder(Drive service, String folderId, boolean recursive) throws IOException {
		Map<String, Integer> patternCounts = new HashMap<>();
		collectFilePatterns(service, folderId, recursive, patternCounts);

		// Sort by count descending
		Map<String, Integer> sorted = patternCounts.entrySet().stream()
				.sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));

		int totalFiles = sorted.values().stream().mapToInt(Integer::intValue).sum();

		logger.info("=== ANALISI PATTERN FILE ===");
		for (Map.Entry<String, Integer> entry : sorted.entrySet()) {
			logger.info("  {}\t({} file)", entry.getKey(), entry.getValue());
		}
		logger.info("=== TOTALE: {} file, {} pattern ===", totalFiles, sorted.size());
	}

	private static void collectFilePatterns(Drive service, String folderId, boolean recursive, Map<String, Integer> patternCounts) throws IOException {
		int folderFileCount = 0;
		String pageToken = null;
		do {
			String fileQuery = String.format("'%s' in parents and mimeType!='%s' and trashed=false", folderId, FOLDER_MIME_TYPE);
			FileList fileResult = service.files().list()
					.setQ(fileQuery)
					.setSpaces("drive")
					.setFields("nextPageToken, files(name)")
					.setPageToken(pageToken)
					.setSupportsAllDrives(true)
					.setIncludeItemsFromAllDrives(true)
					.execute();

			List<File> files = fileResult.getFiles();
			if (files != null) {
				folderFileCount += files.size();
				for (File file : files) {
					String pattern = normalizeFileName(file.getName());
					patternCounts.merge(pattern, 1, Integer::sum);
				}
			}
			pageToken = fileResult.getNextPageToken();
		} while (pageToken != null);

		int totalSoFar = patternCounts.values().stream().mapToInt(Integer::intValue).sum();
		logger.info("Scansione cartella {} : {} file trovati (totale finora: {})", folderId, folderFileCount, totalSoFar);

		if (recursive) {
			String folderQuery = String.format("'%s' in parents and mimeType='%s' and trashed=false", folderId, FOLDER_MIME_TYPE);
			FileList folderResult = service.files().list()
					.setQ(folderQuery)
					.setSpaces("drive")
					.setFields("files(id, name)")
					.setSupportsAllDrives(true)
					.setIncludeItemsFromAllDrives(true)
					.execute();

			List<File> folders = folderResult.getFiles();
			if (folders != null) {
				for (File folder : folders) {
					logger.info("Entro nella sottocartella \"{}\"", folder.getName());
					collectFilePatterns(service, folder.getId(), recursive, patternCounts);
				}
			}
		}
	}

	static String normalizeFileName(String fileName) {
		String result = DATE_PATTERN.matcher(fileName).replaceAll("YYYY-MM-DD");
		result = DATE_COMPACT_PATTERN.matcher(result).replaceAll("YYYYMMDD");
		result = PROVINCE_PDF_PATTERN.matcher(result).replaceAll("XX-NN-YYYY-SEQ.pdf");
		return result;
	}

	private static void recoverFiles(Drive service, String sourceFolderId, boolean recursive) throws IOException {
		Path toRecoverPath = Paths.get("torecover.txt");
		if (!Files.exists(toRecoverPath)) {
			logger.fatal("File torecover.txt non trovato nella directory corrente");
			return;
		}

		// Read file names from torecover.txt
		List<String> fileNames = new ArrayList<>();
		try (BufferedReader reader = new BufferedReader(new FileReader(toRecoverPath.toFile()))) {
			String line;
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if (!line.isEmpty() && !line.startsWith("#")) {
					fileNames.add(line);
				}
			}
		}

		if (fileNames.isEmpty()) {
			logger.warn("Nessun file da recuperare in torecover.txt");
			return;
		}

		logger.info("File da recuperare: {}", fileNames.size());

		// Create recover directory
		Path recoverDir = Paths.get("recover");
		Files.createDirectories(recoverDir);

		int found = 0;
		int notFound = 0;

		for (String fileName : fileNames) {
			logger.info("Ricerca file \"{}\" ...", fileName);
			File driveFile = searchFileRecursive(service, sourceFolderId, fileName, recursive);

			if (driveFile == null) {
				logger.warn("File \"{}\" non trovato su Google Drive", fileName);
				notFound++;
				continue;
			}

			logger.info("File \"{}\" trovato (ID: {}), download in corso...", fileName, driveFile.getId());
			Path outputPath = recoverDir.resolve(fileName);
			try (OutputStream out = new FileOutputStream(outputPath.toFile())) {
				service.files().get(driveFile.getId()).executeMediaAndDownloadTo(out);
			}
			logger.info("File \"{}\" scaricato in {}", fileName, outputPath);
			found++;
		}

		logger.info("=== RIEPILOGO RECOVER ===");
		logger.info("File cercati:   {}", fileNames.size());
		logger.info("File scaricati: {}", found);
		logger.info("File non trovati: {}", notFound);
	}

	private static File searchFileRecursive(Drive service, String folderId, String fileName, boolean recursive) throws IOException {
		// Ricerca globale per nome esatto, senza vincolo di parent: una singola chiamata API
		String escapedName = fileName.replace("\\", "\\\\").replace("'", "\\'");
		String query = String.format("name='%s' and mimeType!='%s' and trashed=false", escapedName, FOLDER_MIME_TYPE);

		String pageToken = null;
		do {
			FileList result = service.files().list()
					.setQ(query)
					.setCorpora("allDrives")
					.setSpaces("drive")
					.setFields("nextPageToken, files(id, name)")
					.setPageToken(pageToken)
					.setSupportsAllDrives(true)
					.setIncludeItemsFromAllDrives(true)
					.execute();

			List<File> files = result.getFiles();
			if (files != null && !files.isEmpty()) {
				return files.get(0);
			}
			pageToken = result.getNextPageToken();
		} while (pageToken != null);

		return null;
	}

	/**
	 * Elimina ricorsivamente le cartelle vuote a partire da folderId (bottom-up).
	 * Ritorna il numero di cartelle eliminate.
	 * Non elimina mai la cartella root (folderId passato al primo livello dal main).
	 */
	private static int deleteEmptyFolders(Drive service, String folderId, boolean recursive) throws IOException {
		if (!recursive) {
			return 0;
		}

		int deleted = 0;
		String pageToken = null;
		do {
			String folderQuery = String.format("'%s' in parents and mimeType='%s' and trashed=false", folderId, FOLDER_MIME_TYPE);
			FileList folderResult = service.files().list()
					.setQ(folderQuery)
					.setSpaces("drive")
					.setFields("nextPageToken, files(id, name)")
					.setPageToken(pageToken)
					.setSupportsAllDrives(true)
					.setIncludeItemsFromAllDrives(true)
					.execute();

			List<File> folders = folderResult.getFiles();
			if (folders != null) {
				for (File folder : folders) {
					// Ricorsione bottom-up: prima pulisci le sottocartelle
					deleted += deleteEmptyFolders(service, folder.getId(), true);

					// Verifica se la cartella e' ora vuota (nessun file e nessuna sottocartella)
					if (isFolderEmpty(service, folder.getId())) {
						if (dryRun) {
							logger.info("[DRY RUN] Eliminazione cartella vuota \"{}\" (ID: {})", folder.getName(), folder.getId());
						} else {
							service.files().delete(folder.getId())
									.setSupportsAllDrives(true)
									.execute();
							logger.info("Cartella vuota \"{}\" eliminata (ID: {})", folder.getName(), folder.getId());
						}
						deleted++;
					}
				}
			}
			pageToken = folderResult.getNextPageToken();
		} while (pageToken != null);

		return deleted;
	}

	private static boolean isFolderEmpty(Drive service, String folderId) throws IOException {
		String query = String.format("'%s' in parents and trashed=false", folderId);
		FileList result = service.files().list()
				.setQ(query)
				.setSpaces("drive")
				.setFields("files(id)")
				.setPageSize(1)
				.setSupportsAllDrives(true)
				.setIncludeItemsFromAllDrives(true)
				.execute();
		return result.getFiles() == null || result.getFiles().isEmpty();
	}

	// ==================== GLACIER ====================

	static String normalizeToGroupKey(String fileName) {
		if (fileName == null) return "";
		// Rimuovi estensioni (anche multiple come .log.gz)
		String result = fileName;
		// Rimuovi tutte le estensioni
		while (result.contains(".")) {
			result = result.substring(0, result.lastIndexOf('.'));
		}
		// Rimuovi date dai pattern conosciuti
		result = DATE_PATTERN.matcher(result).replaceAll("");
		result = DATE_COMPACT_PATTERN.matcher(result).replaceAll("");
		// Pulisci separatori residui (trattini/underscore in coda o doppi)
		result = result.replaceAll("[-_]+$", "");
		result = result.replaceAll("^[-_]+", "");
		result = result.replaceAll("[-_]{2,}", "-");
		return result.isEmpty() ? "noname" : result;
	}

	static String extractDateCompactFromFileName(String fileName) {
		if (fileName == null) return null;

		// 1) YYYY-MM-DD o YYYY_MM_DD
		Matcher matcher = DATE_PATTERN.matcher(fileName);
		if (matcher.find()) {
			int y = Integer.parseInt(matcher.group(1));
			int m = Integer.parseInt(matcher.group(2));
			int d = Integer.parseInt(matcher.group(3));
			if (y >= 1900 && y <= 2100 && m >= 1 && m <= 12 && d >= 1 && d <= 31) {
				return matcher.group(1) + matcher.group(2) + matcher.group(3);
			}
		}

		// 2) YYYYMMDD compatto
		Matcher compactMatcher = DATE_COMPACT_PATTERN.matcher(fileName);
		if (compactMatcher.find()) {
			int y = Integer.parseInt(compactMatcher.group(1));
			int m = Integer.parseInt(compactMatcher.group(2));
			int d = Integer.parseInt(compactMatcher.group(3));
			if (y >= 1900 && y <= 2100 && m >= 1 && m <= 12 && d >= 1 && d <= 31) {
				return compactMatcher.group(1) + compactMatcher.group(2) + compactMatcher.group(3);
			}
		}

		// 3) PDF provincia: solo anno
		Matcher provinceMatcher = PROVINCE_PDF_PATTERN.matcher(fileName);
		if (provinceMatcher.find()) {
			String yearStr = provinceMatcher.group(1);
			if (yearStr.length() == 2) yearStr = "20" + yearStr;
			return yearStr + "0101";
		}

		return null;
	}

	private static void downloadDriveFile(Drive service, String fileId, Path localPath) throws IOException {
		try (OutputStream out = new FileOutputStream(localPath.toFile())) {
			service.files().get(fileId)
					.setSupportsAllDrives(true)
					.executeMediaAndDownloadTo(out);
		}
	}

	private static List<java.io.File> createZipArchives(Drive service, List<File> driveFiles, String groupKey,
			long maxZipBytes, Path tempDir) throws IOException {
		// Sort files by date extracted from name
		driveFiles.sort((a, b) -> {
			String dateA = extractDateCompactFromFileName(a.getName());
			String dateB = extractDateCompactFromFileName(b.getName());
			if (dateA == null) dateA = "";
			if (dateB == null) dateB = "";
			return dateA.compareTo(dateB);
		});

		// Find dateMin and dateMax
		String dateMin = null;
		String dateMax = null;
		for (File f : driveFiles) {
			String d = extractDateCompactFromFileName(f.getName());
			if (d != null) {
				if (dateMin == null || d.compareTo(dateMin) < 0) dateMin = d;
				if (dateMax == null || d.compareTo(dateMax) > 0) dateMax = d;
			}
		}
		if (dateMin == null) dateMin = "00000000";
		if (dateMax == null) dateMax = "00000000";

		List<java.io.File> zipFiles = new ArrayList<>();
		int partIndex = 0;
		int fileIndex = 0;

		while (fileIndex < driveFiles.size()) {
			partIndex++;
			String zipName;
			if (driveFiles.size() <= 1 || maxZipBytes <= 0) {
				// Single zip expected
				zipName = groupKey + "_" + dateMin + "-" + dateMax + ".zip";
			} else {
				zipName = groupKey + "_" + dateMin + "-" + dateMax + "_part" + partIndex + ".zip";
			}

			Path zipPath = tempDir.resolve(zipName);
			long currentZipSize = 0;
			int filesInThisPart = 0;

			try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipPath.toFile()))) {
				while (fileIndex < driveFiles.size()) {
					File driveFile = driveFiles.get(fileIndex);
					Long fileSize = driveFile.getSize();
					long size = (fileSize != null) ? fileSize : 0;

					// Check if adding this file would exceed maxZipBytes (allow at least one file per zip)
					if (filesInThisPart > 0 && maxZipBytes > 0 && currentZipSize + size > maxZipBytes) {
						break;
					}

					// Download to temp
					Path tempFile = tempDir.resolve(driveFile.getName());
					logger.debug("Download file \"{}\" (ID: {})", driveFile.getName(), driveFile.getId());
					downloadDriveFile(service, driveFile.getId(), tempFile);

					// Add to zip
					ZipEntry entry = new ZipEntry(driveFile.getName());
					zos.putNextEntry(entry);
					try (FileInputStream fis = new FileInputStream(tempFile.toFile())) {
						byte[] buffer = new byte[8192];
						int len;
						while ((len = fis.read(buffer)) > 0) {
							zos.write(buffer, 0, len);
						}
					}
					zos.closeEntry();

					// Clean up temp file
					Files.deleteIfExists(tempFile);

					currentZipSize += size;
					filesInThisPart++;
					fileIndex++;
				}
			}

			// If we created an empty part due to split logic, rename without _partN
			if (partIndex == 1 && fileIndex >= driveFiles.size()) {
				// Only one part was needed, rename to remove _partN suffix
				String finalName = groupKey + "_" + dateMin + "-" + dateMax + ".zip";
				Path finalPath = tempDir.resolve(finalName);
				if (!zipPath.equals(finalPath)) {
					Files.move(zipPath, finalPath);
					zipPath = finalPath;
				}
			}

			zipFiles.add(zipPath.toFile());
			logger.info("Creato archivio ZIP \"{}\" ({} file)", zipPath.getFileName(), filesInThisPart);
		}

		return zipFiles;
	}

	private static void uploadFile(Drive service, String folderId, java.io.File localFile) throws IOException {
		int retry = 0;
		boolean done = false;

		while (retry < Settings.operation.retry && !done) {
			try {
				retry++;
				File fileMetadata = new File();
				fileMetadata.setName(localFile.getName());
				fileMetadata.setParents(Collections.singletonList(folderId));

				com.google.api.client.http.FileContent mediaContent =
						new com.google.api.client.http.FileContent("application/zip", localFile);

				File uploaded = service.files().create(fileMetadata, mediaContent)
						.setFields("id")
						.setSupportsAllDrives(true)
						.execute();

				logger.info("Upload completato: \"{}\" (ID: {})", localFile.getName(), uploaded.getId());
				done = true;
			} catch (IOException e) {
				logger.warn("Tentativo {}/{} di upload fallito: {}", retry, Settings.operation.retry, e.getMessage());
				if (retry >= Settings.operation.retry) throw e;
				logger.info("Nuovo tentativo tra {} secondi", Settings.operation.sleepRetry);
				try {
					Thread.sleep(Settings.operation.sleepRetry * 1000L);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	private static void glacierFolder(Drive service, String folderId, String relativePath, String untilYearMonth) throws IOException {
		glacierFolder(service, folderId, relativePath, untilYearMonth, null);
	}

	private static void glacierFolder(Drive service, String folderId, String relativePath,
			String untilYearMonth, String currentYear) throws IOException {
		// List subfolders (con paginazione)
		String pageToken = null;
		do {
			String folderQuery = String.format("'%s' in parents and mimeType='%s' and trashed=false", folderId, FOLDER_MIME_TYPE);
			FileList folderResult = service.files().list()
					.setQ(folderQuery)
					.setSpaces("drive")
					.setFields("nextPageToken, files(id, name)")
					.setPageToken(pageToken)
					.setSupportsAllDrives(true)
					.setIncludeItemsFromAllDrives(true)
					.execute();

			List<File> folders = folderResult.getFiles();
			if (folders != null) {
				for (File folder : folders) {
					String folderName = folder.getName();

					if (isYearFolder(folderName)) {
						// Recurse with year context
						glacierFolder(service, folder.getId(), relativePath, untilYearMonth, folderName);
					} else if (isMonthFolder(folderName) && currentYear != null) {
						String folderYearMonth = currentYear + "-" + folderName;
						if (folderYearMonth.compareTo(untilYearMonth) <= 0) {
							// This folder qualifies for glacier
							logger.info("Glacier: elaborazione cartella {}/{} (relativePath={})", currentYear, folderName, relativePath);
							processGlacierMonth(service, folder.getId(), currentYear, folderName, relativePath);
						} else {
							logger.debug("Glacier: skip cartella {}/{} (successiva a {})", currentYear, folderName, untilYearMonth);
						}
					} else {
						// Non-date subfolder: recurse with relativePath
						String newRelativePath = relativePath.isEmpty() ? folderName : relativePath + "/" + folderName;
						glacierFolder(service, folder.getId(), newRelativePath, untilYearMonth, currentYear);
					}
				}
			}
			pageToken = folderResult.getNextPageToken();
		} while (pageToken != null);
	}

	private static void processGlacierMonth(Drive service, String monthFolderId, String year, String month,
			String relativePath) throws IOException {
		// Collect all files in this month folder (and subfolders for relativePath structure)
		List<File> allFiles = new ArrayList<>();
		collectFilesRecursive(service, monthFolderId, allFiles);

		if (allFiles.isEmpty()) {
			logger.debug("Nessun file nella cartella {}/{}", year, month);
			return;
		}

		logger.info("Trovati {} file in {}/{}{}", allFiles.size(), year, month,
				relativePath.isEmpty() ? "" : "/" + relativePath);

		// Group by normalizeToGroupKey
		Map<String, List<File>> groups = new HashMap<>();
		for (File f : allFiles) {
			String key = normalizeToGroupKey(f.getName());
			groups.computeIfAbsent(key, k -> new ArrayList<>()).add(f);
		}

		long maxZipBytes = Settings.glacier.maxZipSizeMB * 1024L * 1024L;

		for (Map.Entry<String, List<File>> entry : groups.entrySet()) {
			String groupKey = entry.getKey();
			List<File> groupFiles = entry.getValue();

			logger.info("Glacier gruppo \"{}\" : {} file", groupKey, groupFiles.size());

			// Create temp directory
			Path tempDir = Files.createTempDirectory("glacier_");
			try {
				// Create ZIP archives
				List<java.io.File> zips = createZipArchives(service, groupFiles, groupKey, maxZipBytes, tempDir);

				// Ensure glacier destination path: glacier.id/YYYY/MM/[relativePath]
				List<String> pathSegments = new ArrayList<>();
				pathSegments.add(year);
				pathSegments.add(month);
				if (!relativePath.isEmpty()) {
					for (String seg : relativePath.split("/")) {
						if (!seg.isEmpty()) pathSegments.add(seg);
					}
				}
				String glacierFolderId = ensureRemotePath(service, Settings.folder.glacier.id, pathSegments);

				// Upload ZIPs
				for (java.io.File zip : zips) {
					uploadFile(service, glacierFolderId, zip);
					glacierZipsCreated.incrementAndGet();
				}

				// Delete original files from Drive
				for (File f : groupFiles) {
					logger.debug("Eliminazione file originale \"{}\" (ID: {})", f.getName(), f.getId());
					service.files().delete(f.getId())
							.setSupportsAllDrives(true)
							.execute();
					glacierFilesArchived.incrementAndGet();
				}
			} finally {
				// Cleanup temp directory
				try {
					Files.walk(tempDir)
							.sorted((a, b) -> b.compareTo(a))
							.forEach(p -> {
								try { Files.deleteIfExists(p); } catch (IOException ignored) {}
							});
				} catch (IOException ignored) {}
			}
		}
	}

	private static void collectFilesRecursive(Drive service, String folderId, List<File> result) throws IOException {
		// Collect files
		String pageToken = null;
		do {
			String fileQuery = String.format("'%s' in parents and mimeType!='%s' and trashed=false", folderId, FOLDER_MIME_TYPE);
			FileList fileResult = service.files().list()
					.setQ(fileQuery)
					.setSpaces("drive")
					.setFields("nextPageToken, files(id, name, size)")
					.setPageToken(pageToken)
					.setSupportsAllDrives(true)
					.setIncludeItemsFromAllDrives(true)
					.execute();

			if (fileResult.getFiles() != null) {
				result.addAll(fileResult.getFiles());
			}
			pageToken = fileResult.getNextPageToken();
		} while (pageToken != null);

		// Recurse into subfolders
		String folderQuery = String.format("'%s' in parents and mimeType='%s' and trashed=false", folderId, FOLDER_MIME_TYPE);
		FileList folderResult = service.files().list()
				.setQ(folderQuery)
				.setSpaces("drive")
				.setFields("files(id, name)")
				.setSupportsAllDrives(true)
				.setIncludeItemsFromAllDrives(true)
				.execute();

		if (folderResult.getFiles() != null) {
			for (File folder : folderResult.getFiles()) {
				collectFilesRecursive(service, folder.getId(), result);
			}
		}
	}

	// ==================== MOVE ====================

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

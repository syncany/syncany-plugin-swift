/*
 * Syncany, www.syncany.org
 * Copyright (C) 2011-2014 Philipp C. Heckel <philipp.heckel@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.syncany.plugins.swift;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.syncany.config.Config;
import org.syncany.plugins.swift.SwiftTransferSettings;
import org.syncany.plugins.swift.SwiftTransferManager.SwiftReadAfterWriteConsistentFeatureExtension;
import org.syncany.plugins.transfer.AbstractTransferManager;
import org.syncany.plugins.transfer.StorageException;
import org.syncany.plugins.transfer.TransferManager;
import org.syncany.plugins.transfer.features.ReadAfterWriteConsistent;
import org.syncany.plugins.transfer.features.ReadAfterWriteConsistentFeatureExtension;
import org.syncany.plugins.transfer.files.ActionRemoteFile;
import org.syncany.plugins.transfer.files.CleanupRemoteFile;
import org.syncany.plugins.transfer.files.DatabaseRemoteFile;
import org.syncany.plugins.transfer.files.MultichunkRemoteFile;
import org.syncany.plugins.transfer.files.RemoteFile;
import org.syncany.plugins.transfer.files.SyncanyRemoteFile;
import org.syncany.plugins.transfer.files.TempRemoteFile;
import org.syncany.plugins.transfer.files.TransactionRemoteFile;

/**
 * Implements a {@link TransferManager} based on an Dropbox storage backend for the
 * {@link SwiftTransferPlugin}.
 * <p>
 * <p>Using an {@link SwiftTransferSettings}, the transfer manager is configured and uses
 * a well defined Samba share and folder to store the Syncany repository data. While repo and
 * master file are stored in the given folder, databases and multichunks are stored
 * in special sub-folders:
 * <p>
 * <ul>
 * <li>The <tt>databases</tt> folder keeps all the {@link DatabaseRemoteFile}s</li>
 * <li>The <tt>multichunks</tt> folder keeps the actual data within the {@link MultiChunkRemoteFile}s</li>
 * </ul>
 * <p>
 * <p>All operations are auto-connected, i.e. a connection is automatically
 * established.
 *
 * @author Christian Roth <christian.roth@port17.de>
 */
@ReadAfterWriteConsistent(extension = SwiftReadAfterWriteConsistentFeatureExtension.class)
public class SwiftTransferManager extends AbstractTransferManager {
	private static final Logger logger = Logger.getLogger(SwiftTransferManager.class.getSimpleName());

	private final Account account;
	private final Container container;
	private final String multichunksPath;
	private final String databasesPath;
	private final String actionsPath;
	private final String transactionsPath;
	private final String tempPath;

	public SwiftTransferManager(SwiftTransferSettings settings, Config config) {
		super(settings, config);

		boolean usingKeystone = false;
		
		AccountConfig authConfig = new AccountConfig();
		authConfig.setUsername(settings.getUsername());
		authConfig.setPassword(settings.getPassword());
		authConfig.setAuthUrl(settings.getAuthUrl());
		
		if(settings.getTenantName() != null && !settings.getTenantName().equals("")) {
			authConfig.setTenantName(settings.getTenantName());
			usingKeystone = true;
		}
		
		if(settings.getTenantId() != null && !settings.getTenantId().equals("")) {
			authConfig.setTenantId(settings.getTenantId());
			usingKeystone = true;
		}
		
		if(settings.getPreferredRegion() != null && !settings.getPreferredRegion().equals("")) {
			authConfig.setPreferredRegion(settings.getPreferredRegion());
		}
		
		if(usingKeystone) {
			authConfig.setAuthenticationMethod(AuthenticationMethod.KEYSTONE);
		}

		this.account = new AccountFactory(authConfig).createAccount();

		this.container = account.getContainer(settings.getContainer());
		this.multichunksPath = "multichunks";
		this.databasesPath = "databases";
		this.actionsPath = "actions";
		this.transactionsPath = "transactions";
		this.tempPath = "temp";
	}

	@Override
	public void connect() throws StorageException {
		// make a connect
		try {
			logger.log(Level.INFO, "Using swift account (quota {0} bytes used)", new Object[]{this.account.getBytesUsed()});
		} catch (Exception e) {
			throw new StorageException("Unable to connect to swift", e);
		}
	}

	@Override
	public void disconnect() {
		// Nothing
	}

	@Override
	public void init(boolean createIfRequired) throws StorageException {
		connect();

		try {
			if (!testTargetExists() && createIfRequired) {
				this.container.create();
			}
		} catch (Exception e) {
			throw new StorageException("init: Cannot create required directories", e);
		} finally {
			disconnect();
		}
	}

	@Override
	public void download(RemoteFile remoteFile, File localFile) throws StorageException {
		String remotePath = getRemoteFile(remoteFile);

		if (!remoteFile.getName().equals(".") && !remoteFile.getName().equals("..")) {
			try {
				// Download file
				File tempFile = createTempFile(localFile.getName());

				if (logger.isLoggable(Level.INFO)) {
					logger.log(Level.INFO, "Swift: Downloading {0} to temp file {1}", new Object[]{remotePath, tempFile});
				}

				this.container.getObject(remotePath).downloadObject(tempFile);

				// Move file
				if (logger.isLoggable(Level.INFO)) {
					logger.log(Level.INFO, "Swift: Renaming temp file {0} to file {1}", new Object[]{tempFile, localFile});
				}

				localFile.delete();
				FileUtils.moveFile(tempFile, localFile);
				tempFile.delete();
			} catch (IOException ex) {
				logger.log(Level.SEVERE, "Error while downloading file " + remoteFile.getName(), ex);
				throw new StorageException(ex);
			}
		}
	}

	@Override
	public void upload(File localFile, RemoteFile remoteFile) throws StorageException {
		String remotePath = getRemoteFile(remoteFile);
		String tempRemotePath = "temp-" + remoteFile.getName();

		StoredObject tempObject = this.container.getObject(tempRemotePath);
		if (logger.isLoggable(Level.INFO)) {
			logger.log(Level.INFO, "Swift: Uploading {0} to temp file {1}", new Object[]{localFile, tempRemotePath});
		}

		tempObject.uploadObject(localFile);

		// Move
		if (logger.isLoggable(Level.INFO)) {
			logger.log(Level.INFO, "Swift: Renaming temp file {0} to file {1}", new Object[]{tempRemotePath, remotePath});
		}

		tempObject.copyObject(this.container, this.container.getObject(remotePath));
		tempObject.delete();
	}

	@Override
	public boolean delete(RemoteFile remoteFile) throws StorageException {
		String remotePath = getRemoteFile(remoteFile);

		this.container.getObject(remotePath).delete();
		return true;
	}

	@Override
	public void move(RemoteFile sourceFile, RemoteFile targetFile) throws StorageException {
		String sourceRemotePath = getRemoteFile(sourceFile);
		String targetRemotePath = getRemoteFile(targetFile);

		StoredObject fromObject = this.container.getObject(sourceRemotePath);
		StoredObject toObject = this.container.getObject(targetRemotePath);
		fromObject.copyObject(this.container, toObject);
		fromObject.delete();
	}

	@Override
	public <T extends RemoteFile> Map<String, T> list(Class<T> remoteFileClass) throws StorageException {
		// List folder
		String remoteFilePath = getRemoteFilePath(remoteFileClass);

		Collection<StoredObject> listing = this.container.list(remoteFilePath, null, -1);

		// Create RemoteFile objects
		Map<String, T> remoteFiles = new HashMap<>();

		for (StoredObject child : listing) {
			T remoteFile = RemoteFile.createRemoteFile(child.getBareName(), remoteFileClass);
			remoteFiles.put(child.getBareName(), remoteFile);
		}

		return remoteFiles;
	}

	private String getRemoteFile(RemoteFile remoteFile) {
		return getRemoteFilePath(remoteFile.getClass()) + "/" + remoteFile.getName();
	}

	@Override
	public String getRemoteFilePath(Class<? extends RemoteFile> remoteFile) {
		if (remoteFile.equals(MultichunkRemoteFile.class)) {
			return multichunksPath;
		}
		else if (remoteFile.equals(DatabaseRemoteFile.class) || remoteFile.equals(CleanupRemoteFile.class)) {
			return databasesPath;
		}
		else if (remoteFile.equals(ActionRemoteFile.class)) {
			return actionsPath;
		}
		else if (remoteFile.equals(TransactionRemoteFile.class)) {
			return transactionsPath;
		}
		else if (remoteFile.equals(TempRemoteFile.class)) {
			return tempPath;
		}
		else {
			return "";
		}
	}

	@Override
	public boolean testTargetCanWrite() {
		try {
			if (testTargetExists()) {
				File tempFile = File.createTempFile("syncany-write-test", "tmp");

				this.container.getObject("/syncany-write-test").uploadObject(tempFile);
				this.container.getObject("/syncany-write-test").delete();

				tempFile.delete();

				logger.log(Level.INFO, "testTargetCanWrite: Can write, test file created/deleted successfully.");
				return true;
			} else {
				logger.log(Level.INFO, "testTargetCanWrite: Can NOT write, target does not exist.");
				return false;
			}
		} catch (IOException e) {
			logger.log(Level.INFO, "testTargetCanWrite: Can NOT write to target.", e);
			return false;
		}
	}

	@Override
	public boolean testTargetExists() {
		if (this.container.exists()) {
			logger.log(Level.INFO, "testTargetExists: Target does exist.");
			return true;
		} else {
			logger.log(Level.INFO, "testTargetExists: Target does NOT exist.");
			return false;
		}
	}

	@Override
	public boolean testTargetCanCreate() {
		return true;
	}

	@Override
	public boolean testRepoFileExists() {
		try {
			String repoFilePath = getRemoteFile(new SyncanyRemoteFile());

			if (this.container.getObject(repoFilePath).exists()) {
				logger.log(Level.INFO, "testRepoFileExists: Repo file exists at " + repoFilePath);
				return true;
			} else {
				logger.log(Level.INFO, "testRepoFileExists: Repo file DOES NOT exist at " + repoFilePath);
				return false;
			}
		} catch (Exception e) {
			logger.log(Level.INFO, "testRepoFileExists: Exception when trying to check repo file existence.", e);
			return false;
		}
	}

	public static class SwiftReadAfterWriteConsistentFeatureExtension implements ReadAfterWriteConsistentFeatureExtension {
		private final SwiftTransferManager swiftTransferManager;

		public SwiftReadAfterWriteConsistentFeatureExtension(SwiftTransferManager swiftTransferManager) {
			this.swiftTransferManager = swiftTransferManager;
		}

		@Override
		public boolean exists(RemoteFile remoteFile) throws StorageException {
			return swiftTransferManager.container.getObject(swiftTransferManager.getRemoteFile(remoteFile)).exists();
		}
	}
}

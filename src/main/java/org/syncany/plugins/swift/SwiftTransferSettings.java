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

import org.simpleframework.xml.Element;
import org.syncany.plugins.transfer.Encrypted;
import org.syncany.plugins.transfer.Setup;
import org.syncany.plugins.transfer.TransferSettings;

/**
 * @author Christian Roth <christian.roth@port17.de>
 */
public class SwiftTransferSettings extends TransferSettings {

	@Element(name = "authUrl", required = true)
	@Setup(order = 1, description = "Url to authenticate at the Swift cluster")
	public String authUrl;

	@Element(name = "username", required = true)
	@Setup(order = 2, description = "Username")
	public String username;

	@Element(name = "password", required = true)
	@Setup(order = 3, sensitive = true, description = "Password")
	@Encrypted
	public String password;

	@Element(name = "container", required = true)
	@Setup(order = 4, description = "Target container to use")
	public String container;

	public String getAuthUrl() {
		return authUrl;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public String getContainer() {
		return container;
	}

}

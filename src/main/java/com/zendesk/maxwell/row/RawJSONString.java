package com.zendesk.maxwell.row;

// we wrap up raw json here and use the type of this class to allow
// tunneling pre-serialized JSON data through a RowMap

import java.io.Serializable;
import java.util.Objects;

public class RawJSONString implements Serializable {
	private static final long serialVersionUID = -5600187114417848732L;

	public final String json;

	public RawJSONString(String json) {
		this.json = json;
	}

	@Override
	public boolean equals(Object that) {
		if ( !(that instanceof RawJSONString) )
			return false;

		return Objects.equals(this.json, ((RawJSONString) that).json);
	}
}

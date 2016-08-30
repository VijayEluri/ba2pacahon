package org.gost19.ba2veda;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Resources
{
	public List<Resource> resources = new ArrayList<Resource>();

	Resources()
	{

	}

	Resources add(Resource rc)
	{
		resources.add(rc);
		return this;
	}

	Resources add(String _data, String _lang)
	{
		if (_data != null)
		{
			_data = _data.replace("\\", "\\\\");
			resources.add(new Resource(_data, 2, _lang));
		}
		return this;
	}

	Resources add(String _data, int _type)
	{
		if (_data != null)
		{
			if (_type == Resource._String)
				_data = _data.replace("\\", "\\\\");

			resources.add(new Resource(_data, _type));
		}
		return this;
	}

	Resources add(Date _data)
	{
		if (_data != null)
		{
			resources.add(new Resource(util.date2string(_data), Resource._Datetime));
		}
		return this;
	}
}
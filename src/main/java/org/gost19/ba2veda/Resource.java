package org.gost19.ba2veda;

public class Resource
{
	public static final int _Uri = 1;
	public static final int _String = 2;
	public static final int _Integer = 4;
	public static final int _Datetime = 8;
	public static final int _Decimal = 32;
	public static final int _Bool = 64;

	String data;
	String lang = "NONE";
	int type;

	Resource(String _data, int _type)
	{
		data = _data;
		type = _type;
	}

	Resource(String _data, int _type, String _lang)
	{
		data = _data;
		type = _type;
		lang = _lang;
	}
}

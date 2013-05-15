package org.gost19.ba2pacahon;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;

public class util
{
	private static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.sss");
	private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	public static String escape(String string)
	{

		if (string != null)
		{
			StringBuilder sb = new StringBuilder();

			char c = ' ';

			for (int i = 0; i < string.length(); i++)
			{

				c = string.charAt(i);

				if (c == '\n')
				{
					sb.append("\\n");
				} else if (c == '\r')
				{
					sb.append("\\r");
				} else if (c == '\\')
				{
					sb.append("\\\\");
				} else if (c == '"')
				{
					sb.append("\\\"");
				} else if (c == '\t')
				{
					sb.append("\\t");
				} else
				{
					sb.append(c);
				}
			}
			return sb.toString();
		} else
		{
			return "";
		}
	}

	/**
	 * Transforms String to Date.
	 * 
	 * @param date
	 * @param time
	 * @return XMLGregorianCalendar
	 */
	public static Date string2date(String date)
	{
		date = date.replace('T', ' ');
		date = date.substring(0, date.indexOf('+'));
		GregorianCalendar gcal = new GregorianCalendar();
		try
		{
			if (date.length() < 22)
				gcal.setTime(sdf2.parse(date));
			else
				gcal.setTime(sdf1.parse(date));
			return gcal.getTime();
		} catch (Exception ex)
		{
			ex.hashCode();
		}
		return null;
	}

}

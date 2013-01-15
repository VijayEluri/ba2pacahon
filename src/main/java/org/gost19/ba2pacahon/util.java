package org.gost19.ba2pacahon;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;

import net.n3.nanoxml.IXMLElement;

public class util
{
	private static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.sss");
	private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	public static String[] getRecordIdAndTemplateIdOfDocId__OnDate(String id, Date date, Connection connection) throws Exception
	{
		String result[];
		if (date == null)
		{
			return getRecordIdAndTemplateIdOfDocId(id, connection);
		}
		String queryStr = "SELECT recordId, templateId FROM objects WHERE objectId = ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1";
		PreparedStatement ps = connection.prepareStatement(queryStr);
		ps.setString(1, id);
		ps.setLong(2, date.getTime());
		ResultSet rs = ps.executeQuery();
		if (rs.next())
		{
			result = new String[2];
			result[0] = rs.getString(1);
			result[1] = rs.getString(2);
		} else
		{
			result = getRecordIdAndTemplateIdOfDocId(id, connection);
		}
		rs.close();
		ps.close();
		return result;
	}

	public static String[] getRecordIdAndTemplateIdOfDocId(String id, Connection connection) throws Exception
	{
		String result[];
		String queryStr = "SELECT recordId, templateId FROM objects WHERE objectId = ? AND timestamp IS NULL";
		PreparedStatement ps = connection.prepareStatement(queryStr);
		ps.setString(1, id);
		ResultSet rs = ps.executeQuery();
		if (rs.next())
		{
			result = new String[2];
			result[0] = rs.getString(1);
			result[1] = rs.getString(2);
		} else
		{
			return null;
		}
		rs.close();
		ps.close();
		return result;
	}

	public static String getObjectContentOnDate(String id, Date date, Connection connection) throws Exception
	{
		if (date == null)
		{
			return getObjectContent(id, connection);
		}
		String queryStr = "SELECT content FROM objects WHERE objectId = ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1";
		PreparedStatement ps = connection.prepareStatement(queryStr);
		ps.setString(1, id);
		ps.setLong(2, date.getTime());
		ResultSet rs = ps.executeQuery();
		String result = null;
		if (rs.next())
		{
			result = rs.getString(1);
			rs.close();
			ps.close();
		} else
		{
			result = getObjectContent(id, connection);
		}
		return result;
	}

	public static String getObjectContent(String id, Connection connection) throws Exception
	{
		String result;
		String queryStr = "SELECT content FROM objects WHERE objectId = ? AND timestamp IS NULL";
		PreparedStatement ps = connection.prepareStatement(queryStr);
		ps.setString(1, id);
		ResultSet rs = ps.executeQuery();
		if (rs.next())
		{
			result = rs.getString(1);
		} else
		{
			throw new Exception("NoSuchObjectException");
		}
		rs.close();
		ps.close();
		return result;
	}

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

	public static String get(IXMLElement el, String Name, String def_val)
	{
		String res = null;

		IXMLElement dd = el.getFirstChildNamed(Name);

		if (dd != null)
			res = dd.getContent();

		if (res == null)
			return def_val;
		else
			return res;
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

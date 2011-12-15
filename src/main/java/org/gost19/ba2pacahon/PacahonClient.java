package org.gost19.ba2pacahon;

import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.util.UUID;

import org.zeromq.ZMQ;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFReader;
import com.hp.hpl.jena.rdf.model.RDFWriter;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class PacahonClient
{
	ZMQ.Context ctx;
	ZMQ.Socket socket;

	PacahonClient(String connectTo)
	{
		if (connectTo == null)
			connectTo = "tcp://172.17.4.64:5555";

		ctx = ZMQ.context(1);
		socket = ctx.socket(ZMQ.REQ);

		socket.connect(connectTo);
	}

	public String get_ticket(String login, String credential)
	{
		String ticket = null;

		UUID msg_uuid = UUID.randomUUID();

		String msg = "msg:M" + msg_uuid.toString() + "\n" + "rdf:type msg:Message ;\n" + "msg:sender \"client2\" ;\n"
				+ "msg:reciever \"pacahon\" ;\n" + "msg:command \"get_ticket\" ;\n" + "msg:args\n" + "[\n"
				+ "auth:login \"" + login + "\" ;\n" + "auth:credential \"" + credential + "\" ;\n" + "] .\0";

		socket.send(msg.getBytes(), 0);

		byte[] rr = socket.recv(0);

		String result = new String(rr);

		int pos = result.indexOf("auth:ticket");
		if (pos > 0)
		{
			int start_pos = result.indexOf("\"", pos) + 1;
			int end_pos = result.indexOf("\"", start_pos);
			ticket = result.substring(start_pos, end_pos);
		}

		return ticket;
	}

	public boolean put(String ticket, Model data)
	{
		UUID msg_uuid = UUID.randomUUID();

		// преобразуем data в строку
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		RDFWriter w = data.getWriter("N3");
		w.write(data, baos, "");
		String str_data = baos.toString();

		// отрезаем префиксы
		int last_prefix = str_data.lastIndexOf("@prefix");
		if (last_prefix > 0)
		{
			last_prefix = str_data.indexOf("> .", last_prefix);
			str_data = str_data.substring(last_prefix + 3);
		}

		// меняем ["] на [\"]
		str_data = str_data.replaceAll("\"", "\\\\\"");

		// model.write(baos, "N3");

		String msg = "msg:M" + msg_uuid.toString() + "\n" + "rdf:type msg:Message ;\n" + "msg:sender \"client2\" ;\n"
				+ "msg:ticket \"" + ticket + "\" ;\n" + "msg:reciever \"pacahon\" ;\n" + "msg:command \"put\" ;\n"
				+ "msg:args\n" + "\"\"\"" + str_data + "\"\"\" .\0";

		// отправляем
		socket.send(msg.getBytes(), 0);

		byte[] rr = socket.recv(0);

		String result = new String(rr);

		// проверяем все ли ок
		int pos = result.indexOf("msg:status");
		if (pos > 0 && result.indexOf("ok", pos) > 0)
			return true;

		return false;
	}

	public Model get(String ticket, Model arg) throws Exception
	{
		UUID msg_uuid = UUID.randomUUID();

		// преобразуем data в строку
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		RDFWriter w = arg.getWriter("N3");
		w.write(arg, baos, "");
		String str_data = baos.toString();

		// отрезаем префиксы
		int last_prefix = str_data.lastIndexOf("@prefix");
		if (last_prefix > 0)
		{
			last_prefix = str_data.indexOf("> .", last_prefix);
			str_data = str_data.substring(last_prefix + 3);
		}

		// меняем ["] на [\"]
		str_data = str_data.replaceAll("\"", "\\\\\"");

		// model.write(baos, "N3");

		String msg = "msg:M" + msg_uuid.toString() + "\n" + "rdf:type msg:Message ;\n" + "msg:sender \"client2\" ;\n"
				+ "msg:ticket \"" + ticket + "\" ;\n" + "msg:reciever \"pacahon\" ;\n" + "msg:command \"get\" ;\n"
				+ "msg:args\n" + "\"\"\"" + str_data + "\"\"\" .\0";

		// отправляем
		socket.send(msg.getBytes(), 0);

		byte[] rr = socket.recv(0);

		String result = new String(rr, "UTF-8");

		// проверяем все ли ок
		int pos = result.indexOf("msg:status");
		if (pos > 0 && result.indexOf("ok", pos) > 0)
		{
			pos = result.indexOf("msg:result");
			if (pos > 0)
			{
				int start = result.indexOf("\"\"\"", pos);
				int stop = result.indexOf("\"\"\"", start + 3);

				if (stop > 0)
				{
					result = result.substring(start + 3, stop);
					
					// заменить только наружние экранированные кавычки
					result = result.replaceAll("\\\\\"", "\"");
//					result = result.replaceAll("\n", "\\n");

					Model message = ModelFactory.createDefaultModel();

					result = predicates.all_prefixs + result;
					StringReader sr = new StringReader(result);
					RDFReader r = message.getReader("N3");
					String baseURI = "";
					try
					{
						r.read(message, sr, baseURI);
					} catch (Exception ex)
					{
						String result0 = new String(rr, "UTF-8");
						result0.toString();
						throw ex;
					}
					sr.close();
					return message;
				} else
					return null;
			}

		}

		return null;
	}

	public static void main(String[] args) throws Exception
	{
		PacahonClient pacahon_client = new PacahonClient(null);
		String ticket = pacahon_client.get_ticket("user", "9cXsvbvu8=");

		// Model message = ModelFactory.createDefaultModel();
		// message.setNsPrefixes(predicates.getPrefixs());
		// message.add(data);
		// Resource r = message.createResource(predicates.msg+"M123");
		// r.addProperty(ResourceFactory.createProperty(predicates.rdf, "type"),
		// ResourceFactory.createProperty(predicates.msg, "Message"));
		// r.addProperty(ResourceFactory.createProperty(predicates.msg, "sender"), message.createLiteral("client2"));
		// r.addProperty(ResourceFactory.createProperty(predicates.msg, "ticket"), message.createLiteral(ticket));
		// r.addProperty(ResourceFactory.createProperty(predicates.msg, "reciever"), message.createLiteral("pacahon"));
		// r.addProperty(ResourceFactory.createProperty(predicates.msg, "command"),
		// ResourceFactory.createProperty(predicates.msg, "put"));
		// r.addProperty(ResourceFactory.createProperty(predicates.msg, "args"), data);
		Model data = ModelFactory.createDefaultModel();
		data.setNsPrefixes(predicates.getPrefixs());
		Resource r = data.createResource(predicates.zdb + "doc_245e1592-2593-40cd-ae69-3226144e86c1");
		r.addProperty(ResourceFactory.createProperty(predicates.swrc, "name"),
				ResourceFactory.createProperty(predicates.query, "get"));
		r.addProperty(ResourceFactory.createProperty(predicates.gost19, "parentDepartment"),
				ResourceFactory.createProperty(predicates.query, "get"));

		// Triple tt = new Triple(Node.createURI(Predicate.zdb + "doc_123"), Node.createURI(Predicate.rdf__type),
		// Node.createURI(Predicate.swrc__Department));
		// data.add(tt);

		pacahon_client.get(ticket, data);
	}

}

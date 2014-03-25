"""Provides auxiliary methods to rewrite SQL queries.
"""
import time
import urllib2

import dropbox
import sqlparse

from raw import get_token, load_urls, create_tables

TIMEOUT = 90

class RewriterError(Exception):
	pass

def split_resource(resource):
	split = resource.split(':', 1)
	if len(split) == 1:
		return 'dropbox', split[0]
	return split[0], split[1]


def unknown(resource):
	protocol, path = split_resource(resource)
	return protocol, path, dict(), dict()


def csv(resource, sep=',', skiprows=None, header='infer', **kwargs):
	protocol, path = split_resource(resource)
	return protocol, path, dict(sep=sep, skiprows=skiprows, header=header), kwargs


class Rewriter:

	def __init__(self):
		self.clients = {}


	def parse(self, sql):
		stmts = sqlparse.parse(sql)
		if len(stmts) != 1:
			raise RewriterError("Only a single SQL statement is allowed")
		return stmts[0]


	def remove_whitespaces(self, stmt):
		return [tok for tok in stmt.flatten() if tok.ttype != sqlparse.tokens.Token.Text.Whitespace]


	"""Collect tokens with identifiers in the query."""
	def collect_tokens(self, tokens):
			url_tokens = []

			# Find FROM statement
			while True:
				try:
					tok = tokens.pop(0)
				except IndexError:
					raise RewriterError("FROM statement missing")
				if tok.ttype == sqlparse.tokens.Token.Keyword and tok.value.upper() == 'FROM':
					break

			# FIXME: Does not support 'FROM x INNER JOIN y'

			# Parse FROM statement
			while True:
				try:
					tok = tokens.pop(0)
				except IndexError:
					raise RewriterError("FROM badly formed")
				if tok.ttype == sqlparse.tokens.Token.Literal.String.Single:
					url_tokens.append((tok, tok))
					if tokens:
						tok = tokens.pop(0)
						if tok.ttype == sqlparse.tokens.Token.Punctuation and tok.value == ',':
							continue
						elif tok.ttype == sqlparse.tokens.Token.Keyword and tok.value.upper() == 'WHERE':
							return url_tokens
						elif tok.ttype == sqlparse.tokens.Token.Keyword and tok.value.upper() == 'AS':
							try:
								tok = tokens.pop(0)
							except IndexError:
								raise RewriterError("AS badly formed")

							if tok.ttype == sqlparse.tokens.Token.Name:
								if tokens:
									tok = tokens.pop(0)
									if tok.ttype == sqlparse.tokens.Token.Punctuation and tok.value == ',':
										continue
									elif tok.ttype == sqlparse.tokens.Token.Keyword and tok.value.upper() == 'WHERE':
										return url_tokens
									else:
										raise RewriterError("Expected , or WHERE and found '%s'" % tok.value)
								else:
									return url_tokens
							else:
								raise RewriterError("Expected name and found '%s'" % tok.value)
						else:
							raise RewriterError("Excepted , or AS and found '%s'" % tok.value)
					else:
						return url_tokens
				elif tok.ttype == sqlparse.tokens.Token.Name and tok.value.upper() == 'CSV':
					desc = tok.value
					start = tok
					try:
						tok = tokens.pop(0)
					except IndexError:
						raise RewriterError("%s badly formed" % desc)
					if tok.ttype == sqlparse.tokens.Token.Punctuation and tok.value.upper() == '(':
						indent = 1
						while tokens:
							tok = tokens.pop(0)
							if tok.ttype == sqlparse.tokens.Token.Punctuation and tok.value.upper() == '(':
								indent += 1
							elif tok.ttype == sqlparse.tokens.Token.Punctuation and tok.value.upper() == ')':
								indent -= 1
								if indent == 0:
									end = tok
									break
						if indent == 0:
							url_tokens.append((start, end))
							if tokens:
								tok = tokens.pop(0)
								if tok.ttype == sqlparse.tokens.Token.Punctuation and tok.value == ',':
									continue
								elif tok.ttype == sqlparse.tokens.Token.Keyword and tok.value.upper() == 'WHERE':
									return url_tokens
								elif tok.ttype == sqlparse.tokens.Token.Keyword and tok.value.upper() == 'AS':
									try:
										tok = tokens.pop(0)
									except IndexError:
										raise RewriterError("AS badly formed")

									if tok.ttype == sqlparse.tokens.Token.Name:
										if tokens:
											tok = tokens.pop(0)
											if tok.ttype == sqlparse.tokens.Token.Punctuation and tok.value == ',':
												continue
											elif tok.ttype == sqlparse.tokens.Token.Keyword and tok.value.upper() == 'WHERE':
												return url_tokens
											else:
												raise RewriterError("Expected , or WHERE and found '%s'" % tok.value)
										else:
											return url_tokens
									else:
										raise RewriterError("Expected name and found '%s'" % tok.value)
								else:
									raise RewriterError("Excepted , or AS and found '%s'" % tok.value)
							else:
								return url_tokens
						else:
							raise RewriterError("%s arguments badly formed" % desc)
					else:
						raise RewriterError("Expected (")
				else:
					raise RewriterError("Expected filename or function(...) and found '%s'" % tok.value)


	"""Get protocol and resource from identifier."""
	def parse_identifier(self, ident):
		ident = ident.replace("'", '')
		split = ident.split(':', 1)
		if len(split) == 1:
			# Assume Dropbox by default
			return 'dropbox', split[0]

		protocol, resource = split
		return protocol, resource


	"""Get fully-formed URLs from identifier tokens."""
	def get_urls(self, uid, ns, tokens, url_tokens):
		url_with_args, reverse = {}, []
		for start, end in url_tokens:
			if start == end:
				file_type = 'unknown'
				path = start.value.strip("'")
				protocol, path, args, kwargs = unknown(path)
			else:
				file_type = start.value
				all_args = ' '.join([tok.value for tok in tokens[tokens.index(start) + 2:tokens.index(end)]])

				try:
					protocol, path, args, kwargs = eval("""%s(%s)""" % (file_type, all_args), dict(ns, csv=csv))
				except SyntaxError:
					raise RewriterError("Invalid argument syntax %s(%s)" % (file_type, all_args))

			if not protocol in self.clients:
				self.clients[protocol] = get_protocol_client(protocol, uid)	

			url = '%s:%s/%s' % (file_type, protocol, self.clients[protocol].get_full_path(path))
			if args:
				url += '?'
				for key in args:
					value = args[key]
					if value and isinstance(value, str):
						url += '%s=%s&' % (key, urllib2.quote("'%s'" % value))
					elif value:
						url += '%s=%s&' % (key, urllib2.quote(str(value)))
					else:
						url += '%s=None&' % key
				url = url[:-1]

			url_with_args[url] = kwargs
			reverse.append((start, end, url))
		return url_with_args, reverse


	"""Contact RAW service to build database."""
	def build_database(self, uid, url_with_args):
		urls = url_with_args.keys()
		reply = None
		t1 = time.time()
		print '.',
		while True:
			reply = load_urls(uid, urls)
			if reply['errors']:
				msg = ['%s: %s' % (url, reply['errors'][url]) for url in reply['errors']]
				raise RewriterError("Failed processing file(s): " + ' '.join(msg))
			if not reply['pending']:
				break
			if time.time() - t1 > TIMEOUT:
				raise RewriterError("Timed out while building the database")

		print '.', 
		# find a better reply
		# everything is now ready
		# TODO: TEMP HACK
		payload = {}
		for url in url_with_args:
			payload[url] = {'args': url_with_args[url], 'columns': []}
		print '.', 
		reply = create_tables(uid, payload)
		print '.',
		return reply['database'], reply['tables']


	"""Return executable SQL statement."""
	def fix_sql(self, tokens, reverse, tables):
		for start, end, url in reverse:
			ntokens = tokens[:tokens.index(start)]
			ntokens.append(tables[url])
			ntokens += tokens[tokens.index(end) + 1:]
			tokens = ntokens

		sql = ""
		for tok in tokens:
			if isinstance(tok, str) or isinstance(tok, unicode):
				sql += tok
			else:
				sql += tok.value
			sql += " "
		print '.'
		return sql


	def rewrite(self, sql, uid, ns):
		stmt = self.parse(sql)
		tokens = self.remove_whitespaces(stmt)
		url_tokens = self.collect_tokens(tokens[:])
		url_with_args, reverse = self.get_urls(uid, ns, tokens, url_tokens)
		database, tables = self.build_database(uid, url_with_args)
		rewritten_sql = self.fix_sql(tokens, reverse, tables)
		return database, rewritten_sql


"""Get client from protocol."""
def get_protocol_client(protocol, uid):
	if protocol == 'dropbox':
		return DropboxProtocol(uid)
	raise RewriterError("Protocol not supported")


class DropboxProtocol:
	def __init__(self, uid):
		token = get_token(uid)
		self.client = dropbox.client.DropboxClient(token)

	def get_full_path(self, path):
		try:
			rev = self.client.revisions(path, 1)[0]['rev']
		except Exception as ex:
			print ex
			if ex.status == 404:
				raise RewriterError('File not found in Dropbox: %s' % path)
			raise RewriterError('Error contacting Dropbox: %s' % str(ex))
		return '%s/%s' % (path, rev)

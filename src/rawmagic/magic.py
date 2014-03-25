from contextlib import closing

from IPython.core.magic import Magics, magics_class, cell_magic, line_magic, needs_local_scope
from IPython.config.configurable import Configurable
from IPython.utils.traitlets import Bool, Int, Unicode

from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError, OperationalError

from run import run
from rewriter import Rewriter, RewriterError

import sqlparse
import os

class RawMagicError:
	def __init__(self, msg):
		self.msg = msg

	def _repr_html_(self):
		return '<b>Error occurred: </b>%s' % self.msg

	def __str__(self):
		return self.msg


@magics_class
class RawMagic(Magics, Configurable):
	"""Runs SQL statement on a database, specified by SQLAlchemy connect string.

	Provides the %%raw magic."""

	autolimit = Int(0, config=True, help="Automatically limit the size of the returned result sets")
	style = Unicode('DEFAULT', config=True, help="Set the table printing style to any of prettytable's defined styles (currently DEFAULT, MSWORD_FRIENDLY, PLAIN_COLUMNS, RANDOM)")
	short_errors = Bool(True, config=True, help="Don't display the full traceback on SQL Programming Error")
	displaylimit = Int(0, config=True, help="Automatically limit the number of rows displayed (full result set is still stored)")
	autopandas = Bool(True, config=True, help="Return Pandas DataFrames instead of regular result sets")
	feedback = Bool(True, config=True, help="Print number of rows affected by DML")

	def __init__(self, shell):
		Configurable.__init__(self, config=shell.config)
		Magics.__init__(self, shell=shell)

		# Add ourself to the list of module configurable via %config
		self.shell.configurables.append(self)

		self.rewriter = Rewriter()

	@needs_local_scope
	@line_magic('raw')
	@cell_magic('raw')
	def execute(self, line, cell='', local_ns={}):
		# save globals and locals so they can be referenced in bind vars
		user_ns = self.shell.user_ns
		user_ns.update(local_ns)

		uid = int(os.environ.get('DROPBOX_UID'))

		sql = cell
		if not sql:
			sql = line

		try:
			database, rewritten_sql = self.rewriter.rewrite(sql, uid, user_ns)
		except RewriterError as ex:
			return RawMagicError(str(ex))

		engine = create_engine("postgresql://%s:%s@%s/%s" % (database['user'],
								     database['password'],
								     database['host'],
								     database['database']))
		with closing(engine.connect()) as conn:
			try:
				result = run(conn, rewritten_sql, self, user_ns)
				return result
			except (ProgrammingError, OperationalError) as e:
				# Sqlite apparently return all errors as OperationalError :/
				if self.short_errors:
					print(e)
				else:
					raise


def load_ipython_extension(ip):
	"""Load the extension in IPython."""
	ip.register_magics(RawMagic)

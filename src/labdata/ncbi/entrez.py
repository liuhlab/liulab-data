"""A thin, mockable wrapper over Biopython's :mod:`Bio.Entrez`.

Every NCBI E-utilities call in liulab-data goes through :class:`EntrezClient`,
which sets the contact email / API key from
:func:`labdata.ncbi.config.load_credentials` and exposes small typed helpers
(``esearch``, ``esummary``, ``elink``) that parse the XML handle into plain
Python structures. Concentrating network access in one seam keeps the rest of
the package easy to unit-test by substituting a fake client.
"""

from __future__ import annotations

from typing import Any

from Bio import Entrez

from labdata.exceptions import EntrezError
from labdata.ncbi.config import NcbiCredentials, load_credentials

# Bio.Entrez ships no type stubs and exposes mutable module-level globals; alias
# it as Any so the type checker doesn't fight Biopython's dynamic API.
_entrez: Any = Entrez


class EntrezClient:
    """Authenticated entry point to NCBI E-utilities.

    Parameters
    ----------
    credentials : NcbiCredentials or None
        Credentials to use. When ``None`` (default) they are resolved via
        :func:`labdata.ncbi.config.load_credentials`.
    allow_prompt : bool
        Forwarded to :func:`load_credentials` when ``credentials`` is ``None``.
    """

    def __init__(
        self,
        credentials: NcbiCredentials | None = None,
        *,
        allow_prompt: bool = True,
    ) -> None:
        self.credentials = credentials or load_credentials(allow_prompt=allow_prompt)
        _entrez.email = self.credentials.email
        if self.credentials.api_key:
            _entrez.api_key = self.credentials.api_key

    def esearch(self, db: str, term: str, *, retmax: int = 20) -> list[str]:
        """Run an ESearch and return the matching UIDs.

        Parameters
        ----------
        db : str
            Entrez database to search (e.g. ``"gds"``).
        term : str
            The search term.
        retmax : int
            Maximum number of UIDs to return.

        Returns
        -------
        list of str
            The matching UIDs, in NCBI's ranking order.
        """
        record = self._read(_entrez.esearch(db=db, term=term, retmax=retmax))
        return [str(uid) for uid in record.get("IdList", [])]

    def esummary(self, db: str, uid: str) -> dict[str, Any]:
        """Fetch the document summary for a single UID.

        Parameters
        ----------
        db : str
            Entrez database (e.g. ``"gds"`` or ``"sra"``).
        uid : str
            The UID whose summary to fetch.

        Returns
        -------
        dict
            The document summary as a plain dictionary.

        Raises
        ------
        EntrezError
            If NCBI returns no summary for the UID.
        """
        record = self._read(_entrez.esummary(db=db, id=str(uid)))
        if not record:
            raise EntrezError(f"empty esummary for {db} uid {uid!r}")
        return dict(record[0])

    def elink(self, dbfrom: str, db: str, uid: str) -> list[str]:
        """Return UIDs in ``db`` linked from ``uid`` in ``dbfrom``.

        Parameters
        ----------
        dbfrom : str
            Source database (e.g. ``"gds"``).
        db : str
            Target database (e.g. ``"sra"``).
        uid : str
            The source UID.

        Returns
        -------
        list of str
            Linked UIDs in the target database (empty if there are none).
        """
        record = self._read(_entrez.elink(dbfrom=dbfrom, db=db, id=str(uid)))
        linked: list[str] = []
        for linkset in record:
            for linksetdb in linkset.get("LinkSetDb", []):
                linked.extend(str(link["Id"]) for link in linksetdb.get("Link", []))
        return linked

    @staticmethod
    def _read(handle: Any) -> Any:
        """Parse an E-utilities handle, normalizing any failure to EntrezError."""
        try:
            with handle:
                return _entrez.read(handle)
        except Exception as err:
            raise EntrezError(f"NCBI E-utilities request failed: {err}") from err

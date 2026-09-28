from __future__ import annotations
import csv, hashlib, io, json, os, re, sqlite3, threading, time
from difflib import SequenceMatcher
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urljoin, urlparse

import httpx
import fitz
from zoneinfo import ZoneInfo
from fastapi import FastAPI, HTTPException, Query, Request, UploadFile, File
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

BASE = Path(__file__).resolve().parent
if os.getenv("LIESCOPE_DATA_DIR"):
    DATA_HOME = Path(os.environ["LIESCOPE_DATA_DIR"])
elif os.getenv("LOCALAPPDATA"):
    DATA_HOME = Path(os.environ["LOCALAPPDATA"]) / "LieScope"
else:
    DATA_HOME = Path.home() / ".liescope"
DB_PATH = Path(os.getenv("LIESCOPE_DB_PATH", str(DATA_HOME / "liescope.db")))
STATIC = BASE / "static"
PDF_DIR = Path(os.getenv("LIESCOPE_PDF_DIR", str(DATA_HOME / "pdfs")))
EXTRACT_DIR = Path(os.getenv("LIESCOPE_EXTRACT_DIR", str(DATA_HOME / "extracted")))
MAX_PDF_BYTES = int(os.getenv("LIESCOPE_MAX_PDF_BYTES", str(100 * 1024 * 1024)))
ORCID = "0000-0003-4374-9276"
TARGET_NAMES = {"stein atle lie", "stein a lie", "stein a. lie", "s a lie", "sa lie", "stein lie"}
TRUSTED_AFFILIATION_TERMS = (
    "university of bergen", "universitetet i bergen", "department of clinical dentistry",
    "faculty of medicine", "haukeland university hospital", "helse bergen", "bergen, norway"
)
UA = "LieScope/6.0 (research publication explorer; contact via project owner)"
REFRESH_LOCK = threading.Lock()
_scheduler_thread: threading.Thread | None = None
_scheduler_stop = threading.Event()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def norm(s: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()

def normalize_doi(v: str | None) -> str:
    s = (v or "").strip().lower()
    s = re.sub(r"^https?://(dx\.)?doi\.org/", "", s)
    return s

def compact_openalex(v: str | None) -> str:
    s=(v or "").strip()
    return s.rsplit("/",1)[-1] if s else ""

def first_date(d: dict[str, Any]) -> str:
    for key in ("published-print", "published-online", "published", "issued", "created"):
        x=d.get(key)
        try:
            parts=x.get("date-parts", [[]])[0]
            if parts:
                return "-".join([f"{int(parts[0]):04d}"] + [f"{int(z):02d}" for z in parts[1:3]])
        except Exception: pass
    return ""

def exact_target(name: str) -> bool:
    return norm(name) in TARGET_NAMES

def role_for(pos: int | None, total: int | None) -> str:
    if not pos or not total: return "Unknown"
    if total == 1: return "Single"
    if pos == 1: return "First"
    if pos == 2: return "Second"
    if pos == 3: return "Third"
    if pos == total: return "Last"
    if pos == total-1: return "Penultimate"
    return "Middle"

def merge_key(w: dict[str, Any]) -> str:
    if w.get("doi"): return "doi:"+normalize_doi(w["doi"])
    if w.get("pmid"): return "pmid:"+str(w["pmid"])
    if w.get("pmcid"): return "pmcid:"+str(w["pmcid"]).upper()
    if w.get("openalex_id"): return "oa:"+compact_openalex(w["openalex_id"])
    return "title:"+hashlib.sha1((norm(w.get("title"))+"|"+str(w.get("year") or "")).encode()).hexdigest()

def publication_id(key: str) -> str:
    return hashlib.sha1(key.encode()).hexdigest()[:20]

def confidence_for(w: dict[str, Any]) -> tuple[str, int, str]:
    evidence=[]; score=0
    if w.get("orcid_match"):
        score += 70; evidence.append("verified ORCID present in source metadata")
    name_match = any(exact_target(a) for a in w.get("authors",[]))
    if name_match:
        score += 15; evidence.append("target author name present")
    aff = norm(" ".join(w.get("affiliations",[]) or []))
    if any(norm(x) in aff for x in TRUSTED_AFFILIATION_TERMS):
        score += 20; evidence.append("Norwegian/UiB-related affiliation present")
    if len(w.get("sources",[])) >= 2:
        score += 10; evidence.append("record corroborated by multiple sources")
    if w.get("doi"): score += 5
    if w.get("position") and w.get("total_authors"): score += 5
    if score >= 80: return "verified", min(score,100), "; ".join(evidence)
    if score >= 45: return "high", min(score,100), "; ".join(evidence)
    return "review", min(score,100), "; ".join(evidence) or "insufficient identity evidence"

def db_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c=sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory=sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    return c

def init_db():
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
    with db_conn() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS publications(
            id TEXT PRIMARY KEY, merge_key TEXT UNIQUE NOT NULL, title TEXT NOT NULL,
            journal TEXT, year INTEGER, publication_date TEXT, doi TEXT, pmid TEXT, pmcid TEXT, openalex_id TEXT,
            authors_json TEXT NOT NULL DEFAULT '[]', affiliations_json TEXT NOT NULL DEFAULT '[]',
            position INTEGER, total_authors INTEGER, role TEXT, work_type TEXT, is_oa INTEGER DEFAULT 0,
            cited_by INTEGER DEFAULT 0, sources_json TEXT NOT NULL DEFAULT '[]', source_urls_json TEXT NOT NULL DEFAULT '{}',
            confidence_status TEXT NOT NULL DEFAULT 'review', confidence_score INTEGER DEFAULT 0,
            confidence_reason TEXT, user_decision TEXT, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pub_year ON publications(year);
        CREATE INDEX IF NOT EXISTS idx_pub_status ON publications(confidence_status,user_decision);
        CREATE TABLE IF NOT EXISTS update_runs(
            id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT NOT NULL, finished_at TEXT,
            status TEXT NOT NULL, source_summary_json TEXT NOT NULL DEFAULT '{}',
            fetched INTEGER DEFAULT 0, merged INTEGER DEFAULT 0, error TEXT
        );
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS pdf_cache(
            pub_id TEXT PRIMARY KEY, file_path TEXT NOT NULL, source_url TEXT, origin TEXT NOT NULL DEFAULT 'download',
            saved_at TEXT NOT NULL, size_bytes INTEGER NOT NULL DEFAULT 0, sha256 TEXT,
            FOREIGN KEY(pub_id) REFERENCES publications(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS reading_state(
            pub_id TEXT PRIMARY KEY, page INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL,
            FOREIGN KEY(pub_id) REFERENCES publications(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS bookmarks(
            pub_id TEXT PRIMARY KEY, created_at TEXT NOT NULL,
            FOREIGN KEY(pub_id) REFERENCES publications(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS extraction_cache(
            pub_id TEXT PRIMARY KEY, pdf_sha256 TEXT NOT NULL, result_json TEXT NOT NULL, updated_at TEXT NOT NULL,
            FOREIGN KEY(pub_id) REFERENCES publications(id) ON DELETE CASCADE
        );
        """)
        cols={r[1] for r in c.execute("PRAGMA table_info(publications)").fetchall()}
        if "pmcid" not in cols:
            c.execute("ALTER TABLE publications ADD COLUMN pmcid TEXT")
        c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES('profile_photo','')")
        c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES('last_successful_refresh','')")
        c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES('schedule','1st day monthly at 06:00 Europe/Oslo')")

def get_setting(key: str, default="") -> str:
    with db_conn() as c:
        r=c.execute("SELECT value FROM settings WHERE key=?",(key,)).fetchone()
    return r[0] if r else default

def set_setting(key: str, value: str):
    with db_conn() as c:
        c.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(key,value))

async def get_json(client: httpx.AsyncClient, url: str, params: dict[str,Any]|None=None) -> dict:
    r=await client.get(url, params=params, timeout=35)
    r.raise_for_status(); return r.json()

async def fetch_openalex(client: httpx.AsyncClient) -> list[dict]:
    """Resolve every OpenAlex author profile carrying the ORCID, then pull all works for those profiles.
    This is broader than filtering works only by raw ORCID and helps recover older records whose work-level
    metadata may not itself carry the ORCID.
    """
    author_profiles=[]
    try:
        data=await get_json(client,"https://api.openalex.org/authors",{"filter":f"orcid:{ORCID}","per-page":100})
        author_profiles=[compact_openalex(x.get("id")) for x in data.get("results",[]) if x.get("id")]
    except Exception:
        author_profiles=[]
    if not author_profiles:
        try:
            data=await get_json(client,f"https://api.openalex.org/authors/orcid:{ORCID}")
            if data.get("id"): author_profiles=[compact_openalex(data.get("id"))]
        except Exception:
            pass
    rows=[]; seen=set()
    filters=[f"author.id:{aid}" for aid in author_profiles] or [f"authorships.author.orcid:{ORCID}"]
    for flt in filters:
        cursor="*"
        while cursor:
            data=await get_json(client,"https://api.openalex.org/works",{
                "filter":flt,"per-page":200,"cursor":cursor,
                "mailto":os.getenv("LIESCOPE_CONTACT_EMAIL","")
            })
            for x in data.get("results",[]):
                xid=compact_openalex(x.get("id"))
                if xid and xid in seen: continue
                if xid: seen.add(xid)
                auth=[]; aff=[]; pos=None; orcid_match=False
                for i,a in enumerate(x.get("authorships",[]) or [], start=1):
                    name=((a.get("author") or {}).get("display_name") or "").strip(); auth.append(name)
                    ro=(a.get("raw_orcid") or "")
                    ao=((a.get("author") or {}).get("orcid") or "")
                    aid=compact_openalex((a.get("author") or {}).get("id"))
                    target_profile=aid in author_profiles
                    if ORCID in ro or ORCID in ao or target_profile or exact_target(name):
                        if pos is None: pos=i
                        if ORCID in ro or ORCID in ao or target_profile: orcid_match=True
                        for inst in a.get("institutions",[]) or []:
                            nm=inst.get("display_name")
                            if nm and nm not in aff: aff.append(nm)
                loc=x.get("primary_location") or {}; src=loc.get("source") or {}
                doi=normalize_doi(x.get("doi"))
                rows.append({
                    "title":x.get("display_name") or x.get("title") or "Untitled", "journal":src.get("display_name") or "",
                    "year":x.get("publication_year"), "publication_date":x.get("publication_date") or "", "doi":doi,
                    "pmid":"", "pmcid":"", "openalex_id":xid, "authors":auth, "affiliations":aff,
                    "position":pos,"total_authors":len(auth),"role":role_for(pos,len(auth)),"work_type":x.get("type") or "",
                    "is_oa":bool((x.get("open_access") or {}).get("is_oa")),"cited_by":x.get("cited_by_count") or 0,
                    "sources":["OpenAlex"],"source_urls":{
                        "OpenAlex":x.get("id") or "",
                        "OpenAccess":((x.get("best_oa_location") or {}).get("landing_page_url") or ""),
                        "PDF":((x.get("best_oa_location") or {}).get("pdf_url") or "")
                    },"orcid_match":orcid_match
                })
            cursor=(data.get("meta") or {}).get("next_cursor")
            if not data.get("results"): break
    return rows

async def fetch_crossref(client: httpx.AsyncClient) -> list[dict]:
    """Use both exact ORCID filtering and an author-name query. The latter broadens recall for older Crossref
    records that never deposited an ORCID; confidence scoring keeps those candidates from being auto-accepted
    unless other evidence supports the identity.
    """
    rows=[]; seen=set()
    searches=[("filter",f"orcid:{ORCID}"),("query.author","Stein Atle Lie")]
    for mode,value in searches:
        cursor="*"
        for _ in range(60):
            params={"rows":1000,"cursor":cursor,"cursor-max":1000,
                    "select":"DOI,title,author,container-title,published,published-online,published-print,type,URL,volume,issue,page"}
            params[mode]=value
            if os.getenv("LIESCOPE_CONTACT_EMAIL"): params["mailto"]=os.getenv("LIESCOPE_CONTACT_EMAIL")
            d=await get_json(client,"https://api.crossref.org/works",params)
            msg=d.get("message",{}); items=msg.get("items",[]) or []
            for x in items:
                doi=normalize_doi(x.get("DOI")); local_id=doi or norm((x.get("title") or [""])[0])+"|"+str(first_date(x))
                if local_id and local_id in seen: continue
                if local_id: seen.add(local_id)
                auth=[]; aff=[]; pos=None; orcid_match=False
                for i,a in enumerate(x.get("author",[]) or [], start=1):
                    name=" ".join(z for z in [a.get("given",""),a.get("family","")] if z).strip(); auth.append(name)
                    aid=(a.get("ORCID") or "")
                    if ORCID in aid or exact_target(name):
                        if pos is None: pos=i
                        if ORCID in aid: orcid_match=True
                        for af in a.get("affiliation",[]) or []:
                            nm=af.get("name")
                            if nm and nm not in aff: aff.append(nm)
                title=(x.get("title") or ["Untitled"])[0]; journal=(x.get("container-title") or [""])[0]
                date=first_date(x); year=int(date[:4]) if re.match(r"^\d{4}",date) else None
                rows.append({"title":title,"journal":journal,"year":year,"publication_date":date,"doi":doi,"pmid":"","openalex_id":"",
                    "pmcid":"","authors":auth,"affiliations":aff,"position":pos,"total_authors":len(auth),"role":role_for(pos,len(auth)),"work_type":x.get("type") or "",
                    "is_oa":False,"cited_by":0,"sources":["Crossref"],"source_urls":{"Crossref":x.get("URL") or (f"https://doi.org/{doi}" if doi else "")},"orcid_match":orcid_match})
            nxt=msg.get("next-cursor")
            if not items or not nxt or nxt==cursor: break
            cursor=nxt
    return rows

async def fetch_pubmed(client: httpx.AsyncClient) -> list[dict]:
    term=f'"{ORCID}"[auid] OR "Stein Atle Lie"[Author]'
    search=await get_json(client,"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",{"db":"pubmed","term":term,"retmode":"json","retmax":10000})
    ids=(search.get("esearchresult") or {}).get("idlist",[]) or []
    rows=[]
    for start in range(0,len(ids),200):
        chunk=ids[start:start+200]
        r=await client.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",params={"db":"pubmed","id":",".join(chunk),"retmode":"xml"},timeout=45)
        r.raise_for_status()
        import xml.etree.ElementTree as ET
        root=ET.fromstring(r.text)
        for art in root.findall(".//PubmedArticle"):
            med=art.find("MedlineCitation"); article=med.find("Article") if med is not None else None
            if article is None: continue
            pmid=(med.findtext("PMID") or "").strip()
            title="".join(article.find("ArticleTitle").itertext()).strip() if article.find("ArticleTitle") is not None else "Untitled"
            journal=(article.findtext("Journal/Title") or "").strip()
            date=""; year=None
            y=article.findtext("Journal/JournalIssue/PubDate/Year")
            if y and y.isdigit(): year=int(y); date=y
            auth=[]; aff=[]; pos=None; orcid_match=False
            for i,a in enumerate(article.findall("AuthorList/Author"),start=1):
                coll=(a.findtext("CollectiveName") or "").strip()
                name=coll or " ".join(z for z in [(a.findtext("ForeName") or "").strip(),(a.findtext("LastName") or "").strip()] if z)
                auth.append(name)
                ids_el=a.findall("Identifier")
                is_orcid=any((e.attrib.get("Source","").lower()=="orcid" and ORCID in (e.text or "")) for e in ids_el)
                if is_orcid or exact_target(name):
                    if pos is None: pos=i
                    if is_orcid: orcid_match=True
                    for x in a.findall("AffiliationInfo/Affiliation"):
                        nm=(x.text or "").strip()
                        if nm and nm not in aff: aff.append(nm)
            doi=""; pmcid=""
            for eid in art.findall("PubmedData/ArticleIdList/ArticleId"):
                if eid.attrib.get("IdType")=="doi": doi=normalize_doi(eid.text)
                if eid.attrib.get("IdType")=="pmc": pmcid=(eid.text or "").strip()
            rows.append({"title":title,"journal":journal,"year":year,"publication_date":date,"doi":doi,"pmid":pmid,"pmcid":pmcid,"openalex_id":"",
                "authors":auth,"affiliations":aff,"position":pos,"total_authors":len(auth),"role":role_for(pos,len(auth)),"work_type":"journal-article",
                "is_oa":False,"cited_by":0,"sources":["PubMed"],"source_urls":{"PubMed":f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"},"orcid_match":orcid_match})
    return rows


async def fetch_europepmc(client: httpx.AsyncClient) -> list[dict]:
    """High-recall Europe PMC search. Candidate records are still subject to LieScope identity scoring."""
    query='AUTH:"Stein Atle Lie" OR AUTH:"Lie SA"'
    rows=[]; seen=set(); cursor='*'
    for _ in range(40):
        d=await get_json(client,"https://www.ebi.ac.uk/europepmc/webservices/rest/search",{
            "query":query,"format":"json","resultType":"core","pageSize":1000,"cursorMark":cursor
        })
        result=(d.get("resultList") or {}).get("result",[]) or []
        for x in result:
            doi=normalize_doi(x.get("doi")); pmid=str(x.get("pmid") or "")
            key=doi or pmid or str(x.get("id") or "")
            if key and key in seen: continue
            if key: seen.add(key)
            authors=[]; aff=[]; pos=None; orcid_match=False
            al=(x.get("authorList") or {}).get("author",[]) or []
            for i,a in enumerate(al,start=1):
                name=(a.get("fullName") or " ".join(z for z in [a.get("firstName"),a.get("lastName")] if z) or "").strip()
                authors.append(name)
                aid_blob=json.dumps(a.get("authorId") or a.get("authorIdList") or "")
                if ORCID in aid_blob or exact_target(name):
                    if pos is None: pos=i
                    if ORCID in aid_blob: orcid_match=True
                    for af in a.get("authorAffiliationDetailsList",{}).get("authorAffiliation",[]) if isinstance(a.get("authorAffiliationDetailsList"),dict) else []:
                        nm=(af.get("affiliation") or "").strip()
                        if nm and nm not in aff: aff.append(nm)
            j=((x.get("journalInfo") or {}).get("journal") or {}).get("title") or x.get("journalTitle") or ""
            date=x.get("firstPublicationDate") or x.get("firstIndexDate") or str(x.get("pubYear") or "")
            try: year=int(x.get("pubYear") or str(date)[:4])
            except Exception: year=None
            pmcid=str(x.get("pmcid") or "")
            source_urls={"EuropePMC":f"https://europepmc.org/article/{x.get('source') or 'MED'}/{x.get('id') or pmid}"}
            if pmcid: source_urls["PMC"]=f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/"
            rows.append({"title":x.get("title") or "Untitled","journal":j,"year":year,"publication_date":date,
                "doi":doi,"pmid":pmid,"pmcid":pmcid,"openalex_id":"","authors":authors,"affiliations":aff,
                "position":pos,"total_authors":len(authors),"role":role_for(pos,len(authors)),"work_type":x.get("pubType") or "journal-article",
                "is_oa":str(x.get("isOpenAccess") or "").upper()=="Y","cited_by":int(x.get("citedByCount") or 0),
                "sources":["Europe PMC"],"source_urls":source_urls,"orcid_match":orcid_match})
        nxt=d.get("nextCursorMark")
        if not result or not nxt or nxt==cursor: break
        cursor=nxt
    return rows

async def fetch_nva(client: httpx.AsyncClient) -> list[dict]:
    """Query the public NVA search API by contributor name.
    NVA is used as a corroborating source; ambiguous candidates remain in review.
    """
    rows=[]; seen=set(); offset=0
    for _ in range(20):
        d=await get_json(client,"https://api.nva.unit.no/search/resources",{
            "contributor":"Stein Atle Lie","size":1000,"from":offset,"aggregation":"none"
        })
        hits=d.get("hits",[]) or []
        for x in hits:
            title=x.get("mainTitle") or x.get("title") or "Untitled"
            doi=normalize_doi(x.get("doi") or "")
            rid=x.get("id") or ""
            key=doi or rid or norm(title)+"|"+str(x.get("publicationYear") or "")
            if key in seen: continue
            seen.add(key)
            authors=[]; aff=[]; pos=None; orcid_match=False
            for i,c in enumerate(x.get("contributors",[]) or [],start=1):
                ident=c.get("identity") or {}
                name=(ident.get("name") or "").strip()
                if not name:
                    name=" ".join(z for z in [ident.get("firstName"),ident.get("lastName")] if z).strip()
                authors.append(name)
                blob=json.dumps(ident)
                if ORCID in blob or exact_target(name):
                    if pos is None: pos=i
                    if ORCID in blob: orcid_match=True
                    for af in c.get("affiliations",[]) or []:
                        nm=(af.get("name") if isinstance(af,dict) else str(af)) or ""
                        if nm and nm not in aff: aff.append(nm)
            context=x.get("publicationContext") or {}
            journal=(context.get("name") or context.get("title") or x.get("journal") or "") if isinstance(context,dict) else ""
            year=x.get("publicationYear")
            try: year=int(year) if year is not None else None
            except Exception: year=None
            rows.append({"title":title,"journal":journal,"year":year,"publication_date":str(year or ""),"doi":doi,"pmid":"","openalex_id":"",
                "pmcid":"","authors":authors,"affiliations":aff,"position":pos,"total_authors":len(authors),"role":role_for(pos,len(authors)),
                "work_type":((x.get("publicationInstance") or {}).get("type") if isinstance(x.get("publicationInstance"),dict) else "") or "",
                "is_oa":False,"cited_by":0,"sources":["NVA"],"source_urls":{"NVA":rid},"orcid_match":orcid_match})
        if len(hits)<1000: break
        offset += len(hits)
    return rows

def richer(a: dict, b: dict) -> dict:
    out=dict(a)
    for k in ("title","journal","year","publication_date","doi","pmid","pmcid","openalex_id","work_type"):
        if not out.get(k) and b.get(k): out[k]=b[k]
    if len(b.get("authors",[]) or []) > len(out.get("authors",[]) or []):
        out["authors"]=b["authors"]; out["position"]=b.get("position"); out["total_authors"]=b.get("total_authors"); out["role"]=b.get("role")
    out["affiliations"]=list(dict.fromkeys((out.get("affiliations",[]) or [])+(b.get("affiliations",[]) or [])))
    out["sources"]=list(dict.fromkeys((out.get("sources",[]) or [])+(b.get("sources",[]) or [])))
    out["source_urls"]={**(out.get("source_urls") or {}),**(b.get("source_urls") or {})}
    out["orcid_match"]=bool(out.get("orcid_match") or b.get("orcid_match"))
    out["is_oa"]=bool(out.get("is_oa") or b.get("is_oa")); out["cited_by"]=max(int(out.get("cited_by") or 0),int(b.get("cited_by") or 0))
    return out

def _same_work(a: dict[str,Any], b: dict[str,Any]) -> bool:
    da,db=normalize_doi(a.get("doi")),normalize_doi(b.get("doi"))
    if da and db: return da==db
    pa,pb=str(a.get("pmid") or ""),str(b.get("pmid") or "")
    if pa and pb: return pa==pb
    pca,pcb=str(a.get("pmcid") or "").upper(),str(b.get("pmcid") or "").upper()
    if pca and pcb: return pca==pcb
    oa,ob=compact_openalex(a.get("openalex_id")),compact_openalex(b.get("openalex_id"))
    if oa and ob: return oa==ob
    ta,tb=norm(a.get("title")),norm(b.get("title"))
    if not ta or not tb: return False
    ya,yb=a.get("year"),b.get("year")
    if ya and yb and abs(int(ya)-int(yb))>1: return False
    ratio=SequenceMatcher(None,ta,tb).ratio()
    if ratio<0.94: return False
    aa={norm(x) for x in a.get("authors",[]) if x}; bb={norm(x) for x in b.get("authors",[]) if x}
    author_ok=bool(aa & bb) or (any(exact_target(x) for x in a.get("authors",[])) and any(exact_target(x) for x in b.get("authors",[])))
    ja,jb=norm(a.get("journal")),norm(b.get("journal"))
    journal_ok=(not ja or not jb or SequenceMatcher(None,ja,jb).ratio()>=0.72)
    return author_ok and journal_ok

def merge_records(groups: list[list[dict]]) -> list[dict]:
    """Merge across sources using stable identifiers first, then conservative title/year/author/journal matching."""
    merged=[]
    for group in groups:
        for w in group:
            match=None
            # Fast identifier pass.
            for i,m in enumerate(merged):
                if _same_work(m,w): match=i; break
            if match is None: merged.append(dict(w))
            else: merged[match]=richer(merged[match],w)
    return merged

def upsert_publications(rows: list[dict]) -> int:
    ts=now_iso(); count=0
    with db_conn() as c:
        for w in rows:
            key=merge_key(w); rid=publication_id(key)
            status,score,reason=confidence_for(w)
            existing=c.execute("SELECT user_decision,first_seen FROM publications WHERE id=?",(rid,)).fetchone()
            decision=existing["user_decision"] if existing else None; first_seen=existing["first_seen"] if existing else ts
            c.execute("""INSERT INTO publications(id,merge_key,title,journal,year,publication_date,doi,pmid,pmcid,openalex_id,authors_json,affiliations_json,position,total_authors,role,work_type,is_oa,cited_by,sources_json,source_urls_json,confidence_status,confidence_score,confidence_reason,user_decision,first_seen,last_seen,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET merge_key=excluded.merge_key,title=excluded.title,journal=excluded.journal,year=excluded.year,publication_date=excluded.publication_date,doi=excluded.doi,pmid=excluded.pmid,pmcid=excluded.pmcid,openalex_id=excluded.openalex_id,authors_json=excluded.authors_json,affiliations_json=excluded.affiliations_json,position=excluded.position,total_authors=excluded.total_authors,role=excluded.role,work_type=excluded.work_type,is_oa=excluded.is_oa,cited_by=excluded.cited_by,sources_json=excluded.sources_json,source_urls_json=excluded.source_urls_json,confidence_status=excluded.confidence_status,confidence_score=excluded.confidence_score,confidence_reason=excluded.confidence_reason,last_seen=excluded.last_seen,updated_at=excluded.updated_at""",
            (rid,key,w.get("title") or "Untitled",w.get("journal") or "",w.get("year"),w.get("publication_date") or "",normalize_doi(w.get("doi")),w.get("pmid") or "",w.get("pmcid") or "",compact_openalex(w.get("openalex_id")),json.dumps(w.get("authors",[]),ensure_ascii=False),json.dumps(w.get("affiliations",[]),ensure_ascii=False),w.get("position"),w.get("total_authors"),w.get("role") or role_for(w.get("position"),w.get("total_authors")),w.get("work_type") or "",1 if w.get("is_oa") else 0,int(w.get("cited_by") or 0),json.dumps(w.get("sources",[])),json.dumps(w.get("source_urls",{})),status,score,reason,decision,first_seen,ts,ts))
            count+=1
    return count

async def refresh_all() -> dict:
    if not REFRESH_LOCK.acquire(blocking=False):
        return {"status":"busy","message":"A refresh is already running."}
    started=now_iso(); run_id=None
    with db_conn() as c:
        cur=c.execute("INSERT INTO update_runs(started_at,status) VALUES(?,?)",(started,"running")); run_id=cur.lastrowid
    try:
        headers={"User-Agent":UA,"Accept":"application/json"}
        async with httpx.AsyncClient(headers=headers,follow_redirects=True) as client:
            import asyncio
            names=["OpenAlex","Crossref","PubMed","Europe PMC","NVA"]
            fns=[fetch_openalex(client),fetch_crossref(client),fetch_pubmed(client),fetch_europepmc(client),fetch_nva(client)]
            results=await asyncio.gather(*fns,return_exceptions=True)
        groups=[]; summary={}; fetched=0
        for name,res in zip(names,results):
            if isinstance(res,Exception): summary[name]={"status":"error","error":str(res)[:300],"count":0}
            else: summary[name]={"status":"ok","count":len(res)}; groups.append(res); fetched+=len(res)
        if not groups: raise RuntimeError("All scholarly sources failed. Existing database has been left unchanged.")
        merged=merge_records(groups); n=upsert_publications(merged); finished=now_iso(); set_setting("last_successful_refresh",finished)
        with db_conn() as c:
            c.execute("UPDATE update_runs SET finished_at=?,status='success',source_summary_json=?,fetched=?,merged=? WHERE id=?",(finished,json.dumps(summary),fetched,n,run_id))
        return {"status":"success","fetched":fetched,"merged":n,"sources":summary,"finished_at":finished}
    except Exception as e:
        with db_conn() as c:
            c.execute("UPDATE update_runs SET finished_at=?,status='error',error=? WHERE id=?",(now_iso(),str(e)[:1000],run_id))
        raise
    finally:
        REFRESH_LOCK.release()

def row_to_obj(r: sqlite3.Row) -> dict:
    d=dict(r)
    for a,b in (("authors_json","authors"),("affiliations_json","affiliations"),("sources_json","sources"),("source_urls_json","source_urls")):
        try:d[b]=json.loads(d.pop(a) or ("{}" if b=="source_urls" else "[]"))
        except:d[b]={} if b=="source_urls" else []
    decision=d.get("user_decision")
    if decision=="accept": d["display_status"]="accepted"
    elif decision=="exclude": d["display_status"]="excluded"
    elif d.get("confidence_status") in ("verified","high"): d["display_status"]="accepted"
    else:d["display_status"]="review"
    return d


async def fetch_pubmed_article_details(client: httpx.AsyncClient, pmid: str) -> dict[str, Any]:
    """Fetch rich PubMed metadata on demand. This keeps the main catalogue fast while
    allowing the in-app reader to show an abstract, keywords, publication types and PMCID.
    """
    if not pmid:
        return {}
    r = await client.get(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi",
        params={"db":"pubmed","id":pmid,"retmode":"xml"}, timeout=35
    )
    r.raise_for_status()
    import xml.etree.ElementTree as ET
    root = ET.fromstring(r.text)
    art = root.find(".//PubmedArticle")
    if art is None:
        return {}
    med = art.find("MedlineCitation")
    article = med.find("Article") if med is not None else None
    if article is None:
        return {}
    abstract_parts=[]
    for node in article.findall("Abstract/AbstractText"):
        txt="".join(node.itertext()).strip()
        if not txt: continue
        label=(node.attrib.get("Label") or node.attrib.get("NlmCategory") or "").strip()
        abstract_parts.append(f"{label}: {txt}" if label else txt)
    keywords=[]
    for k in med.findall("KeywordList/Keyword") if med is not None else []:
        txt="".join(k.itertext()).strip()
        if txt and txt not in keywords: keywords.append(txt)
    mesh=[]
    for d in med.findall("MeshHeadingList/MeshHeading/DescriptorName") if med is not None else []:
        txt="".join(d.itertext()).strip()
        if txt and txt not in mesh: mesh.append(txt)
    pubtypes=[]
    for n in article.findall("PublicationTypeList/PublicationType"):
        txt="".join(n.itertext()).strip()
        if txt and txt not in pubtypes: pubtypes.append(txt)
    ids={}
    for eid in art.findall("PubmedData/ArticleIdList/ArticleId"):
        typ=(eid.attrib.get("IdType") or "").lower(); val=(eid.text or "").strip()
        if typ and val: ids[typ]=val
    return {
        "abstract":"\n\n".join(abstract_parts), "keywords":keywords, "mesh_terms":mesh,
        "publication_types":pubtypes, "pmcid":ids.get("pmc", ""),
        "doi":normalize_doi(ids.get("doi", "")), "article_ids":ids
    }

def _shorten_text(text: str, limit: int = 900) -> str:
    text=" ".join((text or "").split())
    return text if len(text)<=limit else text[:limit-1].rstrip()+"…"

def _find_article_context(soup, tokens: list[str], limit: int = 900) -> str:
    """Return a nearby article paragraph that explicitly refers to a figure/table label."""
    clean=[t.lower().strip() for t in tokens if t and t.strip()]
    if not clean:
        return ""
    for p in soup.find_all(["p","div"]):
        txt=" ".join(p.stripped_strings) if hasattr(p,"stripped_strings") else ""
        low=txt.lower()
        if len(txt)>35 and any(t in low for t in clean):
            return _shorten_text(txt,limit)
    return ""

def _plain_visual_explanation(kind: str, label: str, caption: str, context: str) -> str:
    """Conservative explanation built only from the article's own caption/context."""
    c=_shorten_text(caption,650)
    x=_shorten_text(context,650)
    lead=f"This {kind.lower()}"
    if label: lead+=f" ({label})"
    if c and x:
        return f"{lead} is described by the article as: {c} Nearby article text adds: {x}"
    if c:
        return f"{lead} is described by the article as: {c}"
    if x:
        return f"{lead} is discussed in the article as follows: {x}"
    return f"{lead} was extracted from the full-text article, but no caption or explanatory context was available to summarize automatically."

async def fetch_pmc_visual_content(client: httpx.AsyncClient, pmcid: str) -> dict:
    """Extract the actual numbered figures and tables from a PubMed Central full-text article.

    Figure images, figure captions, table content, table titles/footnotes and nearby article
    text are returned separately.  The 'explanation' field is intentionally conservative:
    it is generated only from the article's own caption and nearby prose, never from an
    invented interpretation of the underlying results.
    """
    result={"figures":[],"tables":[]}
    if not pmcid:
        return result
    try:
        from bs4 import BeautifulSoup
        base=f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/"
        r=await client.get(base, timeout=35)
        r.raise_for_status()
        soup=BeautifulSoup(r.text,"html.parser")

        # Actual article figures.
        seen=set()
        figure_nodes=[]
        for node in soup.find_all("figure"):
            if node.find("img"): figure_nodes.append(node)
        # Older PMC pages sometimes use div.fig instead of <figure>.
        for node in soup.find_all("div", class_=lambda x: x and "fig" in str(x).lower()):
            if node.find("img") and node not in figure_nodes: figure_nodes.append(node)
        for i,fig in enumerate(figure_nodes):
            img=fig.find("img")
            src=(img.get("src") or img.get("data-src") or img.get("data-original") or "") if img else ""
            if not src: continue
            src=urljoin(base,src)
            if src in seen: continue
            seen.add(src)
            cap=fig.find("figcaption") or fig.find(class_=lambda x: x and "caption" in str(x).lower())
            label=fig.find(class_=lambda x: x and "label" in str(x).lower())
            caption=(" ".join(cap.stripped_strings) if cap else "").strip()
            lbl=(" ".join(label.stripped_strings) if label else "").strip()
            if not lbl:
                # Infer an explicit label only when present at the start of the caption.
                import re
                m=re.match(r"^((?:Fig(?:ure)?\.?)[ ]*\d+[A-Za-z]?)",caption,re.I)
                lbl=m.group(1) if m else f"Figure {len(result['figures'])+1}"
            tokens=[lbl, lbl.replace("Figure","Fig."), lbl.replace("Fig.","Figure")]
            context=_find_article_context(soup,tokens)
            result["figures"].append({
                "image_url":src,"caption":caption,"label":lbl,"context":context,
                "explanation":_plain_visual_explanation("Figure",lbl,caption,context),
                "source_url":base
            })
            if len(result["figures"])>=50: break

        # Actual article tables. PMC commonly wraps them in .table-wrap.
        table_nodes=[]
        for wrap in soup.find_all(["div","figure"], class_=lambda x: x and "table-wrap" in str(x).lower()):
            if wrap.find("table"): table_nodes.append(wrap)
        if not table_nodes:
            # Fallback to tables in article body while avoiding navigation/layout tables.
            article=soup.find("article") or soup
            for t in article.find_all("table"):
                table_nodes.append(t.parent if t.parent else t)
        seen_tables=set()
        for wrap in table_nodes:
            table=wrap.find("table") if getattr(wrap,"name","")!="table" else wrap
            if not table: continue
            # Deduplicate by normalized text.
            signature=" ".join(table.stripped_strings)[:1000]
            if not signature or signature in seen_tables: continue
            seen_tables.add(signature)
            label=wrap.find(class_=lambda x: x and "label" in str(x).lower()) if hasattr(wrap,"find") else None
            cap=(wrap.find("caption") or wrap.find(class_=lambda x: x and "caption" in str(x).lower())) if hasattr(wrap,"find") else None
            lbl=(" ".join(label.stripped_strings) if label else "").strip()
            caption=(" ".join(cap.stripped_strings) if cap else "").strip()
            if not lbl:
                import re
                text=" ".join(wrap.stripped_strings)[:180] if hasattr(wrap,"stripped_strings") else ""
                m=re.search(r"(Table\s+\d+[A-Za-z]?)",text,re.I)
                lbl=m.group(1) if m else f"Table {len(result['tables'])+1}"
            rows=[]
            for tr in table.find_all("tr")[:120]:
                cells=[]
                for cell in tr.find_all(["th","td"]):
                    val=" ".join(cell.stripped_strings).strip()
                    cells.append(val)
                if cells: rows.append(cells)
            footnotes=[]
            for f in wrap.find_all(class_=lambda x: x and ("foot" in str(x).lower() or "tblfn" in str(x).lower())) if hasattr(wrap,"find_all") else []:
                txt=" ".join(f.stripped_strings).strip()
                if txt and txt not in footnotes: footnotes.append(txt)
            tokens=[lbl]
            context=_find_article_context(soup,tokens)
            result["tables"].append({
                "label":lbl,"caption":caption,"rows":rows,
                "footnotes":footnotes[:12],"context":context,
                "explanation":_plain_visual_explanation("Table",lbl,caption,context),
                "source_url":base
            })
            if len(result["tables"])>=50: break
        return result
    except Exception:
        return result

class Decision(BaseModel):
    decision: str
class PhotoSetting(BaseModel):
    url: str = ""

def schedule_due(n: datetime, last_month: str) -> tuple[bool,str]:
    key=f"{n.year:04d}-{n.month:02d}"
    return (n.day==1 and n.hour>=6 and last_month!=key), key

def scheduler_loop():
    import asyncio
    tz=ZoneInfo("Europe/Oslo")
    while not _scheduler_stop.is_set():
        try:
            n=datetime.now(tz)
            last=get_setting("last_scheduled_month","")
            due,key=schedule_due(n,last)
            if due:
                try:
                    asyncio.run(refresh_all())
                    set_setting("last_scheduled_month",key)
                except Exception:
                    pass
        except Exception:
            pass
        _scheduler_stop.wait(60)


def _pdf_cache_row(pub_id: str) -> dict[str, Any] | None:
    with db_conn() as c:
        r=c.execute("SELECT * FROM pdf_cache WHERE pub_id=?",(pub_id,)).fetchone()
    if not r: return None
    d=dict(r)
    p=Path(d.get("file_path") or "")
    if not p.exists():
        with db_conn() as c: c.execute("DELETE FROM pdf_cache WHERE pub_id=?",(pub_id,))
        return None
    return d

def _save_pdf_cache(pub_id: str, path: Path, source_url: str, origin: str):
    data=path.read_bytes()
    with db_conn() as c:
        c.execute("""INSERT INTO pdf_cache(pub_id,file_path,source_url,origin,saved_at,size_bytes,sha256)
                     VALUES(?,?,?,?,?,?,?) ON CONFLICT(pub_id) DO UPDATE SET
                     file_path=excluded.file_path,source_url=excluded.source_url,origin=excluded.origin,
                     saved_at=excluded.saved_at,size_bytes=excluded.size_bytes,sha256=excluded.sha256""",
                  (pub_id,str(path),source_url,origin,now_iso(),len(data),hashlib.sha256(data).hexdigest()))

def _local_pdf_status(pub_id: str) -> dict[str, Any]:
    r=_pdf_cache_row(pub_id)
    if not r: return {"cached":False,"local_url":"","source_url":"","origin":"","saved_at":"","size_bytes":0}
    return {"cached":True,"local_url":f"/api/publications/{pub_id}/pdf-file","source_url":r.get("source_url") or "",
            "origin":r.get("origin") or "download","saved_at":r.get("saved_at") or "","size_bytes":r.get("size_bytes") or 0}

async def _pmc_pdf_candidate(client: httpx.AsyncClient, pmcid: str) -> str:
    if not pmcid: return ""
    base=f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/"
    try:
        r=await client.get(base,timeout=25)
        r.raise_for_status()
        from bs4 import BeautifulSoup
        soup=BeautifulSoup(r.text,"html.parser")
        candidates=[]
        for a in soup.find_all("a",href=True):
            href=(a.get("href") or "").strip()
            txt=" ".join(a.stripped_strings).lower()
            if href.lower().endswith(".pdf") or " pdf" in (" "+txt) or "/pdf/" in href.lower():
                candidates.append(urljoin(str(r.url),href))
        for u in candidates:
            if urlparse(u).scheme == "https": return u
    except Exception:
        pass
    # PMC's /pdf/ route commonly redirects to the article PDF and is safe to try.
    return base+"pdf/"

async def _pdf_download_candidate(obj: dict[str,Any], pmcid: str="") -> tuple[str,str]:
    urls=obj.get("source_urls") or {}
    oa_pdf=(urls.get("PDF") or "").strip()
    # Prefer PMC because it has explicit full-text infrastructure and stable legal OA delivery.
    if pmcid:
        async with httpx.AsyncClient(headers={"User-Agent":UA},follow_redirects=True) as client:
            candidate=await _pmc_pdf_candidate(client,pmcid)
            if candidate: return candidate,"PubMed Central"
    # Then use OpenAlex's best OA PDF metadata when the work is marked OA.
    if oa_pdf and obj.get("is_oa") and urlparse(oa_pdf).scheme=="https":
        return oa_pdf,"OpenAlex open-access location"
    # Optional Unpaywall lookup by DOI. The public API requires a contact email.
    doi=normalize_doi(obj.get("doi"))
    email=os.getenv("LIESCOPE_CONTACT_EMAIL","").strip()
    if doi and email:
        try:
            async with httpx.AsyncClient(headers={"User-Agent":UA},follow_redirects=True) as client:
                u=await get_json(client,f"https://api.unpaywall.org/v2/{quote(doi)}",{"email":email})
                loc=u.get("best_oa_location") or {}
                cand=(loc.get("url_for_pdf") or "").strip()
                if cand and urlparse(cand).scheme=="https": return cand,"Unpaywall open-access location"
        except Exception:
            pass
    return "",""

async def _download_pdf_to_cache(pub_id: str, obj: dict[str,Any], pmcid: str="") -> dict[str,Any]:
    existing=_local_pdf_status(pub_id)
    if existing["cached"]: return existing | {"downloaded":False}
    url,origin=await _pdf_download_candidate(obj,pmcid)
    if not url:
        raise HTTPException(404,"No legally downloadable open-access PDF was found. You can upload a PDF you already have access to.")
    if urlparse(url).scheme!="https": raise HTTPException(400,"Only HTTPS PDF sources are allowed")
    dest=PDF_DIR/f"{pub_id}.pdf"; tmp=PDF_DIR/f".{pub_id}.part"
    total=0
    try:
        async with httpx.AsyncClient(headers={"User-Agent":UA},follow_redirects=True,timeout=60) as client:
            async with client.stream("GET",url) as r:
                r.raise_for_status()
                if urlparse(str(r.url)).scheme!="https": raise HTTPException(400,"Unsafe PDF redirect")
                with tmp.open("wb") as f:
                    first=True
                    async for chunk in r.aiter_bytes(1024*256):
                        if not chunk: continue
                        if first:
                            first=False
                            if not chunk.lstrip().startswith(b"%PDF-"):
                                raise HTTPException(422,"The source did not return a PDF file")
                        total += len(chunk)
                        if total > MAX_PDF_BYTES: raise HTTPException(413,"PDF exceeds the configured size limit")
                        f.write(chunk)
        tmp.replace(dest)
        _save_pdf_cache(pub_id,dest,url,origin)
        with db_conn() as c: c.execute("DELETE FROM extraction_cache WHERE pub_id=?",(pub_id,))
        return _local_pdf_status(pub_id) | {"downloaded":True}
    except Exception:
        try: tmp.unlink(missing_ok=True)
        except Exception: pass
        raise



def _extraction_dir(pub_id: str) -> Path:
    p=EXTRACT_DIR/pub_id
    p.mkdir(parents=True,exist_ok=True)
    return p

def _safe_extracted_path(pub_id: str, rel: str) -> Path:
    base=_extraction_dir(pub_id).resolve()
    target=(base/rel).resolve()
    if base not in target.parents and target!=base:
        raise HTTPException(400,"invalid extraction path")
    return target

def _pdf_sha(pub_id: str) -> str:
    r=_pdf_cache_row(pub_id)
    return (r or {}).get("sha256") or ""

def _read_extraction_cache(pub_id: str) -> dict[str,Any] | None:
    sha=_pdf_sha(pub_id)
    if not sha: return None
    with db_conn() as c:
        r=c.execute("SELECT pdf_sha256,result_json FROM extraction_cache WHERE pub_id=?",(pub_id,)).fetchone()
    if not r or r["pdf_sha256"]!=sha: return None
    try: return json.loads(r["result_json"])
    except Exception: return None

def _save_extraction_cache(pub_id: str, result: dict[str,Any]):
    sha=_pdf_sha(pub_id)
    if not sha: return
    with db_conn() as c:
        c.execute("INSERT INTO extraction_cache(pub_id,pdf_sha256,result_json,updated_at) VALUES(?,?,?,?) ON CONFLICT(pub_id) DO UPDATE SET pdf_sha256=excluded.pdf_sha256,result_json=excluded.result_json,updated_at=excluded.updated_at",
                  (pub_id,sha,json.dumps(result,ensure_ascii=False),now_iso()))

def _find_caption_blocks(page, kind: str) -> list[tuple[fitz.Rect,str]]:
    out=[]
    pat=re.compile(r"^\s*(fig(?:ure)?\.?\s*\d+[A-Za-z]?|table\s*\d+[A-Za-z]?)\b",re.I)
    for b in page.get_text("blocks"):
        if len(b)<5: continue
        txt=" ".join(str(b[4]).split())
        m=pat.match(txt)
        if not m: continue
        low=m.group(1).lower()
        if kind=="figure" and not low.startswith("fig"): continue
        if kind=="table" and not low.startswith("table"): continue
        out.append((fitz.Rect(b[:4]),txt))
    return out

def _nearby_text(page, rect: fitz.Rect, label: str) -> str:
    texts=[]
    for b in page.get_text("blocks"):
        if len(b)<5: continue
        br=fitz.Rect(b[:4]); txt=" ".join(str(b[4]).split())
        if not txt or txt.lower().startswith(label.lower()): continue
        # Prefer paragraphs near the caption, without claiming they directly explain the visual.
        if abs(br.y0-rect.y1) < 220 or abs(rect.y0-br.y1) < 220:
            if len(txt)>35: texts.append(txt)
    return _shorten_text(" ".join(texts[:3]),900)

def _page_crop(page, clip: fitz.Rect, out_path: Path):
    clip=clip & page.rect
    if clip.is_empty: return False
    pix=page.get_pixmap(matrix=fitz.Matrix(1.8,1.8),clip=clip,alpha=False)
    pix.save(str(out_path)); return True

def extract_pdf_content(pub_id: str, force: bool=False) -> dict[str,Any]:
    """Extract figures/tables directly from a locally stored lawful PDF.
    This is layout-based, not OCR-based. Low-confidence cases are labelled and fall back to page crops.
    """
    if not force:
        cached=_read_extraction_cache(pub_id)
        if cached: return cached
    r=_pdf_cache_row(pub_id)
    if not r: raise HTTPException(404,"No saved PDF for this publication")
    pdf_path=Path(r["file_path"])
    outdir=_extraction_dir(pub_id)
    # clean old derived files only for this publication
    for child in outdir.iterdir():
        if child.is_file():
            try: child.unlink()
            except Exception: pass
    figures=[]; tables=[]; page_count=0
    try:
        doc=fitz.open(pdf_path)
        page_count=doc.page_count
        for pno in range(page_count):
            page=doc[pno]
            fig_caps=_find_caption_blocks(page,"figure")
            table_caps=_find_caption_blocks(page,"table")
            # Figure extraction: associate each explicit caption with the closest image rectangle above it.
            image_rects=[]
            for img in page.get_images(full=True):
                xref=img[0]
                try: rects=page.get_image_rects(xref)
                except Exception: rects=[]
                for rr in rects:
                    if rr.width>45 and rr.height>35:
                        image_rects.append(fitz.Rect(rr))
            used=[]
            for cap_i,(cr,caption) in enumerate(fig_caps, start=1):
                m=re.match(r"^\s*((?:Fig(?:ure)?\.?)\s*\d+[A-Za-z]?)",caption,re.I)
                label=m.group(1) if m else f"Figure {len(figures)+1}"
                candidates=[]
                for ir in image_rects:
                    if ir in used: continue
                    # Caption usually lies below the figure; permit overlap for multi-panel/vector composites.
                    vertical=cr.y0-ir.y1
                    horizontal_overlap=max(0,min(cr.x1,ir.x1)-max(cr.x0,ir.x0))
                    score=(abs(vertical) if vertical>=-30 else 9999) - min(horizontal_overlap,200)*0.05
                    if score<650: candidates.append((score,ir))
                if candidates:
                    _,ir=min(candidates,key=lambda x:x[0]); used.append(ir)
                    clip=fitz.Rect(max(0,ir.x0-8),max(0,ir.y0-8),min(page.rect.x1,ir.x1+8),min(page.rect.y1,ir.y1+8))
                    confidence="high" if cr.y0>=ir.y1-35 and cr.y0-ir.y1<220 else "medium"
                else:
                    # Low-confidence fallback: crop the area above the caption instead of pretending exact detection.
                    clip=fitz.Rect(20,max(20,cr.y0-page.rect.height*0.48),page.rect.x1-20,cr.y0-4)
                    confidence="low"
                fname=f"figure-p{pno+1}-{cap_i}.png"
                if _page_crop(page,clip,outdir/fname):
                    context=_nearby_text(page,cr,label)
                    figures.append({"label":label,"caption":caption,"context":context,"explanation":_plain_visual_explanation("Figure",label,caption,context),
                                    "page":pno+1,"confidence":confidence,"source":"saved PDF","image_url":f"/api/publications/{pub_id}/extracted/{fname}"})
            # Table extraction with PyMuPDF's layout engine.
            found=[]
            try:
                tf=page.find_tables()
                found=list(tf.tables or [])
            except Exception:
                found=[]
            for ti,tbl in enumerate(found,start=1):
                try: rows=tbl.extract() or []
                except Exception: rows=[]
                rows=[["" if v is None else str(v) for v in row] for row in rows if row]
                bbox=fitz.Rect(tbl.bbox)
                cap=""; label=f"Table {len(tables)+1}"; confidence="high" if len(rows)>=2 else "medium"
                if table_caps:
                    nearest=min(table_caps,key=lambda z:min(abs(z[0].y0-bbox.y1),abs(bbox.y0-z[0].y1)))
                    if min(abs(nearest[0].y0-bbox.y1),abs(bbox.y0-nearest[0].y1))<180:
                        cap=nearest[1]
                        mm=re.match(r"^\s*(Table\s*\d+[A-Za-z]?)",cap,re.I)
                        if mm: label=mm.group(1)
                context=_nearby_text(page,bbox,label)
                tables.append({"label":label,"caption":cap,"rows":rows,"footnotes":[],"context":context,
                               "explanation":_plain_visual_explanation("Table",label,cap,context),"page":pno+1,
                               "confidence":confidence,"source":"saved PDF"})
            # If there are explicit table captions but no structurally extracted tables, return page crops as low-confidence evidence.
            if not found:
                for ci,(cr,cap) in enumerate(table_caps,start=1):
                    mm=re.match(r"^\s*(Table\s*\d+[A-Za-z]?)",cap,re.I); label=mm.group(1) if mm else f"Table {len(tables)+1}"
                    clip=fitz.Rect(20,cr.y1+2,page.rect.x1-20,min(page.rect.y1-20,cr.y1+page.rect.height*0.42))
                    fname=f"table-p{pno+1}-{ci}.png"
                    if _page_crop(page,clip,outdir/fname):
                        context=_nearby_text(page,cr,label)
                        tables.append({"label":label,"caption":cap,"rows":[],"footnotes":[],"context":context,
                                       "explanation":_plain_visual_explanation("Table",label,cap,context),"page":pno+1,
                                       "confidence":"low","source":"saved PDF","preview_url":f"/api/publications/{pub_id}/extracted/{fname}"})
        doc.close()
    except Exception as e:
        raise HTTPException(422,f"PDF extraction failed: {str(e)[:240]}")
    result={"page_count":page_count,"figures":figures,"tables":tables,"pdf_sha256":_pdf_sha(pub_id),"updated_at":now_iso()}
    _save_extraction_cache(pub_id,result)
    return result

def _paper_at_glance(abstract: str, publication_types: list[str], position: int|None, total: int|None) -> dict[str,str]:
    """Structured summary derived only from labelled abstract sections and explicit metadata."""
    text=(abstract or "").strip()
    sections={}
    for label in ["BACKGROUND","OBJECTIVE","OBJECTIVES","AIM","AIMS","METHODS","METHOD","RESULTS","CONCLUSION","CONCLUSIONS","LIMITATIONS"]:
        m=re.search(rf"(?:^|\n\s*){label}\s*:\s*(.*?)(?=\n\s*[A-Z][A-Z /-]{{2,}}\s*:|$)",text,re.I|re.S)
        if m: sections[label.lower()]=_shorten_text(m.group(1),900)
    question=sections.get("objective") or sections.get("objectives") or sections.get("aim") or sections.get("aims") or ""
    methods=sections.get("methods") or sections.get("method") or ""
    results=sections.get("results") or ""
    conclusions=sections.get("conclusion") or sections.get("conclusions") or ""
    limits=sections.get("limitations") or ""
    design=", ".join(publication_types or [])
    return {"research_question":question,"study_design":design,"sample_data_source":methods,"main_outcome":results,
            "principal_findings":conclusions or results,"limitations":limits,"practical_relevance":conclusions,
            "author_position":f"{position or '?'} of {total or '?'}"}

class ReadingState(BaseModel):
    page: int = 1


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler_thread
    init_db()
    _scheduler_stop.clear()
    if os.getenv("LIESCOPE_DISABLE_SCHEDULER","0")!="1":
        _scheduler_thread=threading.Thread(target=scheduler_loop,name="liescope-monthly-scheduler",daemon=True)
        _scheduler_thread.start()
    yield
    _scheduler_stop.set()

app=FastAPI(title="LieScope API",version="6.0",lifespan=lifespan)
app.mount("/static",StaticFiles(directory=STATIC),name="static")

@app.get("/api/publications/{pub_id}/reading-state")
def reading_state_get(pub_id: str):
    with db_conn() as c:
        r=c.execute("SELECT page,updated_at FROM reading_state WHERE pub_id=?",(pub_id,)).fetchone()
    return {"page":int(r["page"]) if r else 1,"updated_at":r["updated_at"] if r else ""}

@app.post("/api/publications/{pub_id}/reading-state")
def reading_state_set(pub_id: str, body: ReadingState):
    page=max(1,int(body.page or 1))
    with db_conn() as c:
        if not c.execute("SELECT 1 FROM publications WHERE id=?",(pub_id,)).fetchone(): raise HTTPException(404,"publication not found")
        c.execute("INSERT INTO reading_state(pub_id,page,updated_at) VALUES(?,?,?) ON CONFLICT(pub_id) DO UPDATE SET page=excluded.page,updated_at=excluded.updated_at",(pub_id,page,now_iso()))
    return {"ok":True,"page":page}

@app.get("/api/publications/{pub_id}/bookmark")
def bookmark_get(pub_id: str):
    with db_conn() as c: r=c.execute("SELECT created_at FROM bookmarks WHERE pub_id=?",(pub_id,)).fetchone()
    return {"bookmarked":bool(r),"created_at":r["created_at"] if r else ""}

@app.post("/api/publications/{pub_id}/bookmark")
def bookmark_toggle(pub_id: str):
    with db_conn() as c:
        if not c.execute("SELECT 1 FROM publications WHERE id=?",(pub_id,)).fetchone(): raise HTTPException(404,"publication not found")
        r=c.execute("SELECT 1 FROM bookmarks WHERE pub_id=?",(pub_id,)).fetchone()
        if r: c.execute("DELETE FROM bookmarks WHERE pub_id=?",(pub_id,)); state=False
        else: c.execute("INSERT INTO bookmarks(pub_id,created_at) VALUES(?,?)",(pub_id,now_iso())); state=True
    return {"bookmarked":state}

@app.post("/api/publications/{pub_id}/extract-pdf")
def extract_pdf_endpoint(pub_id: str, force: bool=False):
    return extract_pdf_content(pub_id,force=force)

@app.get("/api/publications/{pub_id}/extracted/{rel:path}")
def extracted_asset(pub_id: str, rel: str):
    p=_safe_extracted_path(pub_id,rel)
    if not p.exists() or not p.is_file(): raise HTTPException(404,"extracted asset not found")
    return FileResponse(p)

@app.get("/api/publications/{pub_id}/extraction-status")
def extraction_status(pub_id: str):
    r=_read_extraction_cache(pub_id)
    return {"available":bool(r),"result":r or {}}


@app.get("/",response_class=HTMLResponse)
def root(): return FileResponse(STATIC/"index.html")

@app.get("/api/health")
def health():
    with db_conn() as c:
        n=c.execute("SELECT COUNT(*) FROM publications").fetchone()[0]
    return {"ok":True,"version":"6.0","database":str(DB_PATH),"publication_count":n,"scheduler_enabled":os.getenv("LIESCOPE_DISABLE_SCHEDULER","0")!="1","schedule":get_setting("schedule")}

@app.get("/api/profile")
def profile():
    return {"name":"Stein Atle Lie","orcid":ORCID,"affiliation":"Department of Clinical Dentistry, Faculty of Medicine, University of Bergen, Bergen, Norway","uib_profile":"https://www4.uib.no/en/find-employees/stein-atle.lie","photo_url":get_setting("profile_photo"),"last_successful_refresh":get_setting("last_successful_refresh"),"schedule":get_setting("schedule")}

@app.get("/api/publications")
def publications(status: str = Query("accepted",pattern="^(accepted|review|excluded|all)$"), q: str="", year: int|None=None, role: str="", limit:int=Query(500,ge=1,le=5000), offset:int=Query(0,ge=0)):
    wh=[]; args=[]
    if q:
        wh.append("(lower(title) LIKE ? OR lower(journal) LIKE ? OR lower(authors_json) LIKE ? OR lower(doi) LIKE ? OR lower(pmid) LIKE ?)"); t="%"+q.lower()+"%"; args += [t]*5
    if year: wh.append("year=?"); args.append(year)
    if role: wh.append("role=?"); args.append(role)
    sql="SELECT * FROM publications"+(" WHERE "+" AND ".join(wh) if wh else "")+" ORDER BY COALESCE(publication_date,'') DESC, COALESCE(year,0) DESC, title LIMIT ? OFFSET ?"; args += [limit,offset]
    with db_conn() as c: rows=[row_to_obj(r) for r in c.execute(sql,args).fetchall()]
    if status!="all": rows=[r for r in rows if r["display_status"]==status]
    return {"items":rows,"count":len(rows)}

@app.get("/api/publications/{pub_id}/details")
async def publication_details(pub_id: str):
    with db_conn() as c:
        row=c.execute("SELECT * FROM publications WHERE id=?",(pub_id,)).fetchone()
    if not row:
        raise HTTPException(404,"publication not found")
    obj=row_to_obj(row)
    detail={"abstract":"","keywords":[],"mesh_terms":[],"publication_types":[],"pmcid":"","figures":[],"tables":[]}
    if obj.get("pmid"):
        try:
            async with httpx.AsyncClient(headers={"User-Agent":UA},follow_redirects=True) as client:
                detail.update(await fetch_pubmed_article_details(client,obj["pmid"]))
                if detail.get("pmcid"):
                    visuals=await fetch_pmc_visual_content(client,detail["pmcid"])
                    detail["figures"]=visuals.get("figures",[])
                    detail["tables"]=visuals.get("tables",[])
                    for f in detail["figures"]:
                        f.setdefault("source","PubMed Central structured full text"); f.setdefault("confidence","high")
                    for t in detail["tables"]:
                        t.setdefault("source","PubMed Central structured full text"); t.setdefault("confidence","high")
        except Exception:
            detail["metadata_warning"]="Detailed PubMed/PMC metadata is temporarily unavailable."
    # Saved-PDF extraction is a fallback, never a replacement for higher-quality structured PMC content.
    pdf_status=_local_pdf_status(pub_id)
    pdf_extract=None
    if pdf_status.get("cached"):
        try:
            pdf_extract=extract_pdf_content(pub_id,force=False)
            if not detail["figures"]: detail["figures"]=pdf_extract.get("figures",[])
            if not detail["tables"]: detail["tables"]=pdf_extract.get("tables",[])
        except Exception as e:
            detail["pdf_extraction_warning"]=str(getattr(e,"detail",e))[:300]
    urls=obj.get("source_urls") or {}
    pmcid=detail.get("pmcid") or obj.get("pmcid") or ""
    detail["pmcid"]=pmcid
    pmc_url=f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/" if pmcid else ""
    doi_url=f"https://doi.org/{quote(obj.get('doi') or '')}" if obj.get("doi") else ""
    oa_url=urls.get("OpenAccess") or urls.get("PMC") or ""
    pdf_url=urls.get("PDF") or ""
    publisher_url=doi_url or urls.get("Crossref") or oa_url
    candidate_url,candidate_origin=await _pdf_download_candidate(obj,pmcid)
    pdf_status.update({"can_auto_download":bool(candidate_url),"candidate_url":candidate_url,"candidate_origin":candidate_origin,
                       "page_count":(pdf_extract or {}).get("page_count",0)})
    source_rows=[]
    for name,url in (urls or {}).items():
        if url: source_rows.append({"name":name,"url":url})
    if obj.get("pmid"): source_rows.append({"name":"PubMed","url":f"https://pubmed.ncbi.nlm.nih.gov/{obj['pmid']}/"})
    if pmcid: source_rows.append({"name":"PubMed Central","url":pmc_url})
    if doi_url: source_rows.append({"name":"DOI","url":doi_url})
    # Deduplicate source names/URLs while preserving order.
    seen=set(); clean=[]
    for r in source_rows:
        k=(r["name"],r["url"])
        if k not in seen: seen.add(k); clean.append(r)
    detail.update({
        "publication":obj,
        "full_text_url":pmc_url or oa_url or publisher_url,
        "in_app_full_text_url":pmc_url,
        "pdf_url":pdf_url,
        "pdf_cache":pdf_status,
        "publisher_url":publisher_url,
        "pubmed_url":f"https://pubmed.ncbi.nlm.nih.gov/{obj['pmid']}/" if obj.get("pmid") else "",
        "openalex_url":f"https://openalex.org/{obj['openalex_id']}" if obj.get("openalex_id") else "",
        "paper_at_glance":_paper_at_glance(detail.get("abstract") or "",detail.get("publication_types") or [],obj.get("position"),obj.get("total_authors")),
        "source_provenance":clean,
        "pdf_extraction":pdf_extract or {}
    })
    return detail

@app.get("/api/publications/{pub_id}/pdf-status")
async def pdf_status(pub_id: str):
    with db_conn() as c: row=c.execute("SELECT * FROM publications WHERE id=?",(pub_id,)).fetchone()
    if not row: raise HTTPException(404,"publication not found")
    obj=row_to_obj(row); pmcid=obj.get("pmcid") or ""
    if obj.get("pmid"):
        try:
            async with httpx.AsyncClient(headers={"User-Agent":UA},follow_redirects=True) as client:
                pmcid=(await fetch_pubmed_article_details(client,obj["pmid"])).get("pmcid") or ""
        except Exception: pass
    st=_local_pdf_status(pub_id)
    candidate,origin=await _pdf_download_candidate(obj,pmcid)
    return st | {"can_auto_download":bool(candidate),"candidate_url":candidate,"candidate_origin":origin}

@app.post("/api/publications/{pub_id}/cache-pdf")
async def cache_pdf(pub_id: str):
    with db_conn() as c: row=c.execute("SELECT * FROM publications WHERE id=?",(pub_id,)).fetchone()
    if not row: raise HTTPException(404,"publication not found")
    obj=row_to_obj(row); pmcid=obj.get("pmcid") or ""
    if obj.get("pmid"):
        try:
            async with httpx.AsyncClient(headers={"User-Agent":UA},follow_redirects=True) as client:
                pmcid=(await fetch_pubmed_article_details(client,obj["pmid"])).get("pmcid") or ""
        except Exception: pass
    return await _download_pdf_to_cache(pub_id,obj,pmcid)

@app.post("/api/publications/{pub_id}/upload-pdf")
async def upload_pdf(pub_id: str, file: UploadFile = File(...)):
    with db_conn() as c: exists=c.execute("SELECT 1 FROM publications WHERE id=?",(pub_id,)).fetchone()
    if not exists: raise HTTPException(404,"publication not found")
    filename=(file.filename or "paper.pdf").lower()
    data=await file.read(MAX_PDF_BYTES+1)
    if len(data)>MAX_PDF_BYTES: raise HTTPException(413,"PDF exceeds the configured size limit")
    if not data.lstrip().startswith(b"%PDF-"): raise HTTPException(422,"Uploaded file is not a valid PDF")
    dest=PDF_DIR/f"{pub_id}.pdf"; dest.write_bytes(data)
    _save_pdf_cache(pub_id,dest,"uploaded by user","upload")
    with db_conn() as c: c.execute("DELETE FROM extraction_cache WHERE pub_id=?",(pub_id,))
    return _local_pdf_status(pub_id)

@app.get("/api/publications/{pub_id}/pdf-file")
def pdf_file(pub_id: str):
    r=_pdf_cache_row(pub_id)
    if not r: raise HTTPException(404,"No saved PDF for this publication")
    p=Path(r["file_path"])
    return FileResponse(p,media_type="application/pdf",filename=f"liescope-{pub_id}.pdf",content_disposition_type="inline")

@app.delete("/api/publications/{pub_id}/pdf-file")
def delete_pdf(pub_id: str):
    r=_pdf_cache_row(pub_id)
    if r:
        try: Path(r["file_path"]).unlink(missing_ok=True)
        except Exception: pass
    with db_conn() as c:
        c.execute("DELETE FROM pdf_cache WHERE pub_id=?",(pub_id,))
        c.execute("DELETE FROM extraction_cache WHERE pub_id=?",(pub_id,))
    d=EXTRACT_DIR/pub_id
    if d.exists():
        import shutil
        try: shutil.rmtree(d)
        except Exception: pass
    return {"ok":True}

@app.get("/api/stats")
def stats():
    with db_conn() as c: rows=[row_to_obj(r) for r in c.execute("SELECT * FROM publications").fetchall()]
    accepted=[r for r in rows if r["display_status"]=="accepted"]; review=[r for r in rows if r["display_status"]=="review"]; excluded=[r for r in rows if r["display_status"]=="excluded"]
    by_role={}; by_year={}; journals={}; co={}
    for r in accepted:
        by_role[r.get("role") or "Unknown"]=by_role.get(r.get("role") or "Unknown",0)+1
        if r.get("year"): by_year[str(r["year"])]=by_year.get(str(r["year"]),0)+1
        j=r.get("journal") or "Unknown"; journals[j]=journals.get(j,0)+1
        for i,a in enumerate(r.get("authors",[]),start=1):
            if i!=r.get("position") and a: co[a]=co.get(a,0)+1
    top=lambda d,n=20:[{"name":k,"count":v} for k,v in sorted(d.items(),key=lambda x:(-x[1],x[0]))[:n]]
    return {"accepted":len(accepted),"review":len(review),"excluded":len(excluded),"by_role":by_role,"by_year":by_year,"top_journals":top(journals),"top_coauthors":top(co),"this_year":by_year.get(str(datetime.now().year),0)}

@app.post("/api/review/{pub_id}")
def review(pub_id:str, body:Decision):
    if body.decision not in ("accept","exclude","review"): raise HTTPException(400,"decision must be accept, exclude or review")
    val=None if body.decision=="review" else body.decision
    with db_conn() as c:
        cur=c.execute("UPDATE publications SET user_decision=?,updated_at=? WHERE id=?",(val,now_iso(),pub_id))
        if not cur.rowcount: raise HTTPException(404,"publication not found")
    return {"ok":True,"id":pub_id,"decision":body.decision}

@app.post("/api/settings/photo")
def photo(body:PhotoSetting): set_setting("profile_photo",body.url.strip()); return {"ok":True,"url":body.url.strip()}

@app.post("/api/refresh")
async def refresh():
    try:return await refresh_all()
    except Exception as e: raise HTTPException(502,str(e))

@app.get("/api/update-log")
def update_log(limit:int=Query(20,ge=1,le=200)):
    with db_conn() as c: rows=[dict(r) for r in c.execute("SELECT * FROM update_runs ORDER BY id DESC LIMIT ?",(limit,)).fetchall()]
    for r in rows:
        try:r["source_summary"]=json.loads(r.pop("source_summary_json") or "{}")
        except:r["source_summary"]={}
    return rows


@app.get("/api/source-health")
def source_health():
    with db_conn() as c:
        r=c.execute("SELECT * FROM update_runs ORDER BY id DESC LIMIT 1").fetchone()
    summary={}
    if r:
        try: summary=json.loads(r["source_summary_json"] or "{}")
        except Exception: summary={}
    return {"sources":summary,"last_successful_refresh":get_setting("last_successful_refresh"),"schedule":get_setting("schedule")}

@app.get("/api/export.ris")
def export_ris():
    with db_conn() as c: rows=[row_to_obj(r) for r in c.execute("SELECT * FROM publications ORDER BY COALESCE(year,0) DESC,title").fetchall()]
    rows=[r for r in rows if r["display_status"]=="accepted"]
    out=[]
    for r in rows:
        out.append("TY  - JOUR")
        out.append(f"TI  - {r['title']}")
        for a in r.get("authors",[]): out.append(f"AU  - {a}")
        if r.get("journal"): out.append(f"JO  - {r['journal']}")
        if r.get("year"): out.append(f"PY  - {r['year']}")
        if r.get("doi"): out.append(f"DO  - {r['doi']}")
        if r.get("pmid"): out.append(f"AN  - PMID:{r['pmid']}")
        out.append("ER  - "); out.append("")
    data=("\r\n".join(out)).encode("utf-8")
    return StreamingResponse(iter([data]),media_type="application/x-research-info-systems",headers={"Content-Disposition":"attachment; filename=liescope-publications-v6.ris"})

@app.get("/api/export.bib")
def export_bib():
    with db_conn() as c: rows=[row_to_obj(r) for r in c.execute("SELECT * FROM publications ORDER BY COALESCE(year,0) DESC,title").fetchall()]
    rows=[r for r in rows if r["display_status"]=="accepted"]
    chunks=[]
    for i,r in enumerate(rows,1):
        first=(r.get("authors") or ["Lie"])[0]
        key=re.sub(r"[^A-Za-z0-9]+","",first.split()[-1] if first else "Lie")+str(r.get("year") or "")+str(i)
        fields={"title":r.get("title") or "","author":" and ".join(r.get("authors") or []),"journal":r.get("journal") or "","year":str(r.get("year") or ""),"doi":r.get("doi") or ""}
        body=",\n".join(f"  {k} = {{{v.replace('{','').replace('}','')}}}" for k,v in fields.items() if v)
        chunks.append(f"@article{{{key},\n{body}\n}}")
    data=("\n\n".join(chunks)).encode("utf-8")
    return StreamingResponse(iter([data]),media_type="application/x-bibtex",headers={"Content-Disposition":"attachment; filename=liescope-publications-v6.bib"})

@app.get("/api/export.csv")
def export_csv():
    with db_conn() as c: rows=[row_to_obj(r) for r in c.execute("SELECT * FROM publications ORDER BY COALESCE(year,0) DESC,title").fetchall()]
    rows=[r for r in rows if r["display_status"]=="accepted"]
    sio=io.StringIO(); w=csv.writer(sio); w.writerow(["Title","Year","Journal","Position","Total authors","Role","DOI","PMID","PMCID","OpenAlex","Status","Sources","Authors"])
    for r in rows:w.writerow([r["title"],r["year"],r["journal"],r["position"],r["total_authors"],r["role"],r["doi"],r["pmid"],r.get("pmcid",""),r["openalex_id"],r["display_status"],"; ".join(r["sources"]),"; ".join(r["authors"])])
    data=("\ufeff"+sio.getvalue()).encode("utf-8")
    return StreamingResponse(iter([data]),media_type="text/csv",headers={"Content-Disposition":"attachment; filename=liescope-publications-v6.csv"})

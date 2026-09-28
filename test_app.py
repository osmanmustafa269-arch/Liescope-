import os, tempfile, importlib, sys, base64
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi.testclient import TestClient
import fitz
from PIL import Image
import io


def load_app(tmp):
    os.environ['LIESCOPE_DB_PATH']=str(tmp/'test.db')
    os.environ['LIESCOPE_DATA_DIR']=str(tmp/'data')
    os.environ['LIESCOPE_DISABLE_SCHEDULER']='1'
    import app as mod
    importlib.reload(mod)
    mod.init_db()
    return mod


def seed(m, title='Test paper', doi='10.1/abc', pmid='', openalex='W1'):
    row={'title':title,'journal':'Journal','year':2026,'publication_date':'2026-09-01','doi':doi,'pmid':pmid,'openalex_id':openalex,
         'authors':['A','Stein Atle Lie','C'],'affiliations':['University of Bergen'],'position':2,'total_authors':3,'role':'Second',
         'work_type':'article','is_oa':False,'cited_by':1,'sources':['OpenAlex'],'source_urls':{'OpenAlex':f'https://openalex.org/{openalex}'},'orcid_match':True}
    m.upsert_publications([row])
    c=TestClient(m.app)
    return c.get('/api/publications?status=accepted').json()['items'][0]


def make_pdf(path: Path):
    doc=fitz.open(); page=doc.new_page(width=595,height=842)
    # tiny embedded image followed by an explicit caption
    im=Image.new('RGB',(160,100),(240,240,245)); buf=io.BytesIO(); im.save(buf,format='PNG'); png=buf.getvalue()
    page.insert_image(fitz.Rect(90,90,330,240),stream=png)
    page.insert_text((90,265),'Figure 1. Example plotted result used only for automated extraction testing.',fontsize=10)
    page.insert_text((90,292),'Figure 1 shows the example result described in this synthetic paper.',fontsize=10)
    # draw a table with explicit grid
    page.insert_text((70,360),'Table 1. Example participant values.',fontsize=10)
    x0,y0=70,390; widths=[140,100,100]; heights=[28,28,28]
    xs=[x0]
    for w in widths: xs.append(xs[-1]+w)
    ys=[y0]
    for h in heights: ys.append(ys[-1]+h)
    for x in xs: page.draw_line((x,ys[0]),(x,ys[-1]),color=(0,0,0),width=0.8)
    for y in ys: page.draw_line((xs[0],y),(xs[-1],y),color=(0,0,0),width=0.8)
    vals=[['Group','N','Mean'],['A','10','2.1'],['B','12','2.5']]
    for ri,row in enumerate(vals):
        for ci,val in enumerate(row): page.insert_text((xs[ci]+5,ys[ri]+18),val,fontsize=9)
    page.insert_text((70,500),'Table 1 summarizes the synthetic participant values.',fontsize=10)
    doc.save(path); doc.close()


def test_health_profile_and_static(tmp_path):
    m=load_app(tmp_path); c=TestClient(m.app)
    h=c.get('/api/health').json()
    assert h['version']=='6.0'
    assert c.get('/api/profile').json()['orcid']=='0000-0003-4374-9276'
    r=c.get('/')
    assert r.status_code==200 and 'LieScope' in r.text and 'PDF Reader' in r.text and 'Sources' in r.text


def test_merge_confidence_review_and_exports(tmp_path):
    m=load_app(tmp_path)
    a={'title':'Test paper','journal':'J','year':2025,'publication_date':'2025','doi':'10.1/abc','pmid':'','openalex_id':'W1','authors':['A','Stein Atle Lie','B'],'affiliations':['University of Bergen'],'position':2,'total_authors':3,'role':'Second','work_type':'article','is_oa':True,'cited_by':3,'sources':['OpenAlex'],'source_urls':{},'orcid_match':True}
    b=dict(a); b['pmid']='123'; b['sources']=['PubMed']; b['source_urls']={'PubMed':'x'}
    rows=m.merge_records([[a],[b]])
    assert len(rows)==1 and set(rows[0]['sources'])=={'OpenAlex','PubMed'}
    m.upsert_publications(rows); c=TestClient(m.app)
    item=c.get('/api/publications?status=accepted').json()['items'][0]
    assert item['position']==2
    assert c.post(f"/api/review/{item['id']}",json={'decision':'exclude'}).status_code==200
    assert c.get('/api/publications?status=excluded').json()['count']==1
    c.post(f"/api/review/{item['id']}",json={'decision':'accept'})
    assert c.get('/api/export.csv').status_code==200
    assert c.get('/api/export.ris').status_code==200
    assert c.get('/api/export.bib').status_code==200


def test_reading_state_and_bookmark(tmp_path):
    m=load_app(tmp_path); item=seed(m); c=TestClient(m.app)
    assert c.get(f"/api/publications/{item['id']}/reading-state").json()['page']==1
    assert c.post(f"/api/publications/{item['id']}/reading-state",json={'page':7}).json()['page']==7
    assert c.get(f"/api/publications/{item['id']}/reading-state").json()['page']==7
    assert c.get(f"/api/publications/{item['id']}/bookmark").json()['bookmarked'] is False
    assert c.post(f"/api/publications/{item['id']}/bookmark").json()['bookmarked'] is True
    assert c.post(f"/api/publications/{item['id']}/bookmark").json()['bookmarked'] is False


def test_pdf_upload_reader_validation_and_duplicate_checksum(tmp_path):
    m=load_app(tmp_path); item=seed(m,doi='10.1234/pdf',openalex='WPDF'); c=TestClient(m.app)
    fake=b'%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF\n'
    r=c.post(f"/api/publications/{item['id']}/upload-pdf",files={'file':('paper.pdf',fake,'application/pdf')})
    assert r.status_code==200 and r.json()['cached'] is True
    sha1=m._pdf_cache_row(item['id'])['sha256']
    r2=c.post(f"/api/publications/{item['id']}/upload-pdf",files={'file':('paper.pdf',fake,'application/pdf')})
    assert r2.status_code==200 and m._pdf_cache_row(item['id'])['sha256']==sha1
    assert c.get(f"/api/publications/{item['id']}/pdf-file").content.startswith(b'%PDF-')
    bad=c.post(f"/api/publications/{item['id']}/upload-pdf",files={'file':('bad.pdf',b'not pdf','application/pdf')})
    assert bad.status_code==422


def test_pdf_figure_table_extraction_and_details_fallback(tmp_path):
    m=load_app(tmp_path); item=seed(m,doi='10.1234/extract',openalex='WEXT'); c=TestClient(m.app)
    pdf=tmp_path/'synthetic.pdf'; make_pdf(pdf)
    with pdf.open('rb') as f:
        up=c.post(f"/api/publications/{item['id']}/upload-pdf",files={'file':('synthetic.pdf',f,'application/pdf')})
    assert up.status_code==200
    ex=c.post(f"/api/publications/{item['id']}/extract-pdf")
    assert ex.status_code==200, ex.text
    data=ex.json()
    assert data['page_count']==1
    assert len(data['figures'])>=1
    assert data['figures'][0]['page']==1
    assert data['figures'][0]['source']=='saved PDF'
    # PyMuPDF table detection should find the explicit grid. If it does not, low-confidence table crop must exist.
    assert len(data['tables'])>=1
    details=c.get(f"/api/publications/{item['id']}/details").json()
    assert details['pdf_cache']['cached'] is True
    assert details['figures']
    assert details['tables']
    asset=details['figures'][0]['image_url']
    assert c.get(asset).status_code==200


def test_paper_at_glance_is_only_from_labelled_abstract(tmp_path):
    m=load_app(tmp_path)
    g=m._paper_at_glance('OBJECTIVE: Test the question.\nMETHODS: We used registry data.\nRESULTS: X was higher.\nCONCLUSIONS: The observed association remained.', ['Journal Article'],2,5)
    assert g['research_question']=='Test the question.'
    assert 'registry data' in g['sample_data_source']
    assert g['author_position']=='2 of 5'
    empty=m._paper_at_glance('An unstructured sentence only.',[],2,5)
    assert empty['research_question']=='' and empty['principal_findings']==''

def test_scheduler_due_logic(tmp_path):
    m=load_app(tmp_path)
    from datetime import datetime
    due,key=m.schedule_due(datetime(2026,10,1,6,0), '')
    assert due and key=='2026-10'
    assert m.schedule_due(datetime(2026,10,1,5,59), '')[0] is False
    assert m.schedule_due(datetime(2026,10,1,8,0), '2026-10')[0] is False


def test_refresh_all_sources_fail_preserves_database(tmp_path, monkeypatch):
    import asyncio
    m=load_app(tmp_path); seed(m,doi='10.9/preserve',openalex='WP')
    async def fail(_client):
        raise RuntimeError('offline')
    for name in ['fetch_openalex','fetch_crossref','fetch_pubmed','fetch_europepmc','fetch_nva']:
        monkeypatch.setattr(m,name,fail)
    try:
        asyncio.run(m.refresh_all())
        assert False, 'expected refresh failure'
    except RuntimeError:
        pass
    with m.db_conn() as db:
        assert db.execute('SELECT COUNT(*) FROM publications').fetchone()[0]==1

# HTML Documents (Skipped by Default)

The pipeline skips HTML documents by default during download. This page explains why and what you can do about it.

---

## The Problem

Some REGDOCS filings are HTML documents rather than PDFs (receipts, export-order application forms, etc.). These HTML files reference images with relative paths like:

```
images/CER_EN.jpg     (the CER letterhead)
images/yes.png        (checked checkboxes in forms)
images/no.png         (unchecked checkboxes)
```

Those images are stored as separate nodes in CER's OpenText Content Server (at `docs2.cer-rec.gc.ca`), and the server **does not expose them to the public**:

- Resolving the relative image path against the document URL returns a "Content Server - Error / Error opening item" page.
- The image nodes are not listed in REGDOCS folder views or search, so their node IDs (needed for a `File/Download/<id>` link) cannot be discovered.
- The Content Server REST API (`/api/v1/nodes/<id>/nodes`) returns 401 for anonymous users.
- The `ZipAndDownload` action (which would bundle the HTML with its images) returns "Error processing request" for anonymous users.

This is a permission/configuration choice on CER's side — the same documents render with broken images on the live REGDOCS site.

---

## How the Pipeline Handles It

1. **Scout**: Discovers the document and stores `"kind": "Html Document"` in metadata.
2. **Download**: Recognizes HTML documents (by `kind` or detected `.html` extension) and skips them without making a request.
3. **Convert**: If HTML documents are downloaded (via `--include-html`), the converter preprocesses them before Docling conversion:
   - `yes.png` / `checked.png` → ☑
   - `no.png` / `unchecked.png` → ☐
   - Images with `alt` text → replaced by that text
   - All other images → removed

---

## Downloading HTML Documents Anyway

```bash
python regdocs.py download --include-html
```

The documents will download but with broken image references. The converter handles the common cases (form checkmarks), but decorative images (logos, letterhead) will be missing.

---

## What CER Would Need to Change

For the images to be accessible, CER's Content Server administrators would need to do one of:

1. Grant the public/anonymous user "See Contents" + fetch permission on the supporting image nodes inside compound documents.
2. Enable anonymous access to the [Content Server REST API](https://developer.opentext.com/ce/products/extendedecm/apis/contentserver233restapi) so supporting files can be enumerated and fetched.
3. Enable the Zip & Download action for anonymous users (see OpenText [KB0779393](https://support.opentext.com/csm?id=ot_kb_unauthenticated&sysparm_article=KB0779393)) so a document and its images can be retrieved as one archive.

If the missing images matter for your use case, consider reporting it to CER at <regdocs@cer-rec.gc.ca>.

-- CARTA · A Personal Codex · SQLite schema

CREATE TABLE IF NOT EXISTS provinces (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  slug TEXT UNIQUE NOT NULL,
  icon TEXT,
  description TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS articles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  slug TEXT UNIQUE NOT NULL,
  title TEXT NOT NULL,
  deck TEXT,
  province_id INTEGER,
  subheading TEXT,
  wiki_title TEXT,
  wiki_url TEXT,
  wiki_revid INTEGER,
  summary TEXT,
  content_html TEXT,
  lead_image TEXT,
  word_count INTEGER DEFAULT 0,
  read_minutes INTEGER DEFAULT 0,
  source_sha256 TEXT,
  captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (province_id) REFERENCES provinces(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_articles_province  ON articles(province_id);
CREATE INDEX IF NOT EXISTS idx_articles_captured  ON articles(captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_articles_title     ON articles(title);

CREATE TABLE IF NOT EXISTS images (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  article_id INTEGER NOT NULL,
  local_filename TEXT NOT NULL,
  wiki_url TEXT,
  caption TEXT,
  width INTEGER,
  height INTEGER,
  position INTEGER DEFAULT 0,
  is_lead INTEGER DEFAULT 0,
  FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_images_article ON images(article_id);

CREATE TABLE IF NOT EXISTS marginalia (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  article_id INTEGER NOT NULL,
  content TEXT NOT NULL,
  source TEXT DEFAULT 'user',       -- 'user' or 'carta'
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_marginalia_article ON marginalia(article_id);

CREATE TABLE IF NOT EXISTS cross_refs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  article_id INTEGER NOT NULL,
  target_title TEXT NOT NULL,
  target_slug TEXT,
  target_article_id INTEGER,
  context TEXT,
  FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE,
  FOREIGN KEY (target_article_id) REFERENCES articles(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_crossref_article ON cross_refs(article_id);
CREATE INDEX IF NOT EXISTS idx_crossref_target  ON cross_refs(target_article_id);

CREATE TABLE IF NOT EXISTS carta_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  role TEXT NOT NULL,        -- 'user' | 'carta'
  content TEXT NOT NULL,
  session_id TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_cartalog_session ON carta_log(session_id, created_at);

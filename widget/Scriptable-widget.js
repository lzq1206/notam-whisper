// China & nearby rocket map widget for Scriptable on iPhone.
// Paste into Scriptable, run once, then add the Scriptable widget to Home Screen.
const IMAGE_URL = "https://raw.githubusercontent.com/lzq1206/notam-whisper/main/widget/china-square.png";
const PAGE_URL = "https://rocket.rainywhisper.com/widget/";
const REFRESH_HOURS = 3;
const fm = FileManager.local();
const cachePath = fm.joinPath(fm.cacheDirectory(), "rocket-china-widget.png");
const widget = new ListWidget();
widget.setPadding(0, 0, 0, 0);
widget.url = PAGE_URL;

try {
  // A 10-second cache-buster prevents stale CDN/browser copies while the server
  // image itself changes every 3 hours. iOS may refresh later than requested.
  const cacheKey = Math.floor(Date.now() / 10000);
  const request = new Request(`${IMAGE_URL}?v=${cacheKey}`);
  request.headers = { "Cache-Control": "no-cache", "Pragma": "no-cache" };
  request.timeoutInterval = 30;
  const image = await request.loadImage();
  fm.writeImage(cachePath, image);
  widget.backgroundImage = image;
} catch (error) {
  if (fm.fileExists(cachePath)) {
    widget.backgroundImage = fm.readImage(cachePath);
  } else {
    widget.backgroundColor = new Color("#102638");
    const label = widget.addText("地图暂不可用");
    label.textColor = Color.white();
    label.centerAlignText();
  }
  console.warn(`Widget image request failed: ${error}`);
}

widget.refreshAfterDate = new Date(Date.now() + REFRESH_HOURS * 60 * 60 * 1000);
Script.setWidget(widget);
Script.complete();

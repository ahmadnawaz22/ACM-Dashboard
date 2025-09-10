// UI menu so you can open the dashboard without deploying a web app
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Dashboard')
    .addItem('Open (Dialog - wide)', 'openDashboardDialog')
    .addItem('Open (Sidebar - narrow)', 'openDashboardSidebar')
    .addToUi();
}

// Full-width modal (best for your layout)
function openDashboardDialog() {
  const html = HtmlService.createHtmlOutputFromFile('Dashboard')
    .setTitle('Account Management Dashboard')
    .setWidth(1280)
    .setHeight(800);
  SpreadsheetApp.getUi().showModalDialog(html, 'Account Management Dashboard');
}

// Sidebar (narrow; good for quick checks)
function openDashboardSidebar() {
  const html = HtmlService.createHtmlOutputFromFile('Dashboard')
    .setTitle('Account Management Dashboard');
  SpreadsheetApp.getUi().showSidebar(html);
}

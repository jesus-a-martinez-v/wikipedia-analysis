package wikipedia

case class WikipediaArticle(title: String, text: String) {
  def containsWord(word: String): Boolean = text.split(" ").contains(word)
}

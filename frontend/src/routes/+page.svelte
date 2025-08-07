<script lang="ts">
  let query = "";
  let results: { title: string; subtitles: string; url: string }[] = [];

  async function search() {
    if (query.length < 2) {
      results = [];
      return;
    }
    const response = await fetch(`http://localhost:8000/search?q=${query}`);
    const data = await response.json();
    results = data.results || [];
  }
</script>

<main class="container mx-auto p-4">
  <h1 class="text-4xl font-bold mb-4">Criterion Closet Search</h1>
  <input
    type="text"
    bind:value={query}
    on:input={search}
    placeholder="Search subtitles..."
    class="w-full p-2 border rounded mb-4"
  />

  {#if results.length > 0}
    <ul>
      {#each results as result}
        <li class="mb-4 p-4 border rounded">
          <h2 class="text-2xl font-bold">{result.title}</h2>
          <p class="my-2">{result.subtitles}</p>
          <a href={result.url} target="_blank" class="text-blue-500 hover:underline">
            Watch on YouTube
          </a>
        </li>
      {/each}
    </ul>
  {/if}
</main>

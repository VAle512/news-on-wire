package it.uniroma3.newswire.classification;

/**
 * Questa classe da una semantica alle classi in cui vogliamo classificare le nostre osservazioni.
 * Si noti che l'ordine con cui sono state messe è importante perchè talvolta non si può dare una semantica 
 * alle features am sono un numero che le identifichi.
 * @author luigi
 *
 */
public enum WebsiteClass {
	news,
	other,
	section
}
